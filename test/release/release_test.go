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

// Package release holds the checks that keep the release path honest (Task 3.5).
//
// A release is the one thing in this repository that cannot be un-shipped: a
// pushed image tag and a published GitHub Release are visible to everyone the
// moment they exist, and the audience for them is by definition people with no
// other source of information about the project. The parts that decide what gets
// published are therefore worth testing at the same standard as the operator:
//
//   - `hack/changelog-section.sh` decides what the release notes say, and whether
//     the release happens at all. It is the gate, so its failure modes are tested
//     explicitly rather than assumed.
//   - CHANGELOG.md's structure is what the gate reads. A heading style that drifts
//     would not break anything visibly — it would quietly stop matching, and the
//     next release would fail at tag time.
//   - The wiring between the workflow, the make targets and the script is exactly
//     the kind of seam that breaks silently, because nothing exercises it until
//     somebody tags.
//
// None of it needs a cluster, a registry or a database, so it all runs under
// `make test`.
package release

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// repoPath resolves a repository-relative path from this test's own source
// location, so the tests behave identically under `go test ./...` from the
// repository root and under an editor running one test from this directory.
func repoPath(elems ...string) string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot locate the test source file")
	}
	root := filepath.Join(filepath.Dir(thisFile), "..", "..")
	return filepath.Join(append([]string{root}, elems...)...)
}

func readFile(t *testing.T, relPath string) string {
	t.Helper()
	raw, err := os.ReadFile(repoPath(relPath))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return string(raw)
}

const changelogScript = "hack/changelog-section.sh"

// runScript invokes the extractor and returns stdout, stderr and the exit code.
// A signal or a failure to start the process is fatal — that is a broken test
// environment, not a result the caller should interpret as an exit code.
func runScript(t *testing.T, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()

	cmd := exec.Command(repoPath(changelogScript), args...) // #nosec G204 -- test-controlled arguments
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut

	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		exitCode = 0
	case asExitError(err, &exitErr):
		exitCode = exitErr.ExitCode()
	default:
		t.Fatalf("run %s %v: %v", changelogScript, args, err)
	}
	return out.String(), errOut.String(), exitCode
}

// asExitError keeps the switch above readable; errors.As with a typed target
// reads worse inline than it does named.
func asExitError(err error, target **exec.ExitError) bool {
	if e, ok := err.(*exec.ExitError); ok {
		*target = e
		return true
	}
	return false
}

// fixture writes a changelog into a temp directory and returns its path.
func fixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "CHANGELOG.md")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}

// The fixture below is deliberately shaped like the real file — an empty
// Unreleased section, two released versions, and the compare-link block at the
// bottom — because those are the three things the extractor has to get right.
const sampleChangelog = `# Changelog

Preamble prose that belongs to no section.

## [Unreleased]

_Nothing yet._

## [0.2.0] - 2026-09-01

### Added

- A thing.

### Removed

- Another thing.

## [0.1.0] - 2026-08-03

### Added

- The first thing.

[Unreleased]: https://example.invalid/compare/v0.2.0...HEAD
[0.2.0]: https://example.invalid/releases/tag/v0.2.0
[0.1.0]: https://example.invalid/releases/tag/v0.1.0
`

func TestChangelogSection(t *testing.T) {
	changelog := fixture(t, sampleChangelog)

	tests := []struct {
		name         string
		version      string
		wantExit     int
		wantContains []string
		wantAbsent   []string
		wantStderr   string
	}{
		{
			name:         "a released version",
			version:      "v0.2.0",
			wantContains: []string{"### Added", "- A thing.", "### Removed", "- Another thing."},
			// The neighbouring sections must not bleed in: a release body carrying
			// the previous release's notes is worse than a terse one, because it
			// reads as though nothing changed.
			wantAbsent: []string{"The first thing", "Preamble prose", "Nothing yet"},
		},
		{
			name:         "the leading v is optional",
			version:      "0.2.0",
			wantContains: []string{"- A thing."},
		},
		{
			name:    "the oldest section stops before the link block",
			version: "v0.1.0",
			// The compare links sit inside the last section by position and belong
			// to none of them by meaning. GitHub would render them as literal text.
			wantContains: []string{"- The first thing."},
			wantAbsent:   []string{"example.invalid"},
		},
		{
			name:         "build metadata is not part of a section heading",
			version:      "v0.2.0+build.7",
			wantContains: []string{"- A thing."},
		},
		{
			name:         "a prerelease falls back to the version it is a candidate for",
			version:      "v0.2.0-rc.1",
			wantContains: []string{"- A thing."},
			wantStderr:   "prerelease",
		},
		{
			name:       "a version with no section fails the release",
			version:    "v9.9.9",
			wantExit:   1,
			wantStderr: "has no section for v9.9.9",
		},
		{
			name:    "a prerelease of a version with no section fails too",
			version: "v9.9.9-rc.1",
			// Fallback is not a licence to release without notes: the base version
			// has none either, so there is nothing to fall back to.
			wantExit:   1,
			wantStderr: "has no section for v9.9.9-rc.1",
		},
		{
			name:    "Unreleased can never satisfy the gate",
			version: "Unreleased",
			// Exit 2, not 1: this is a malformed argument rather than a missing
			// section, and only the latter is fixed by writing notes.
			wantExit:   2,
			wantStderr: "not a semantic version",
		},
		{
			name:       "a partial version is refused rather than guessed at",
			version:    "v0.2",
			wantExit:   2,
			wantStderr: "not a semantic version",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, exit := runScript(t, tc.version, changelog)

			if exit != tc.wantExit {
				t.Fatalf("exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s", exit, tc.wantExit, stdout, stderr)
			}
			for _, want := range tc.wantContains {
				if !strings.Contains(stdout, want) {
					t.Errorf("stdout does not contain %q:\n%s", want, stdout)
				}
			}
			for _, absent := range tc.wantAbsent {
				if strings.Contains(stdout, absent) {
					t.Errorf("stdout contains %q, which belongs to another section:\n%s", absent, stdout)
				}
			}
			if tc.wantStderr != "" && !strings.Contains(stderr, tc.wantStderr) {
				t.Errorf("stderr does not mention %q:\n%s", tc.wantStderr, stderr)
			}
			if tc.wantExit == 0 && strings.TrimSpace(stdout) == "" {
				t.Error("succeeded with empty notes, which would publish an empty release")
			}
			// Leading and trailing blank lines are trimmed, so the output can be
			// pasted into a release body without a stray gap at either end.
			if tc.wantExit == 0 && stdout != strings.TrimLeft(stdout, "\n") {
				t.Error("output starts with a blank line")
			}
		})
	}
}

// TestChangelogSectionRejectsAnEmptySection is the case that would otherwise slip
// through: a heading exists, so a naive "does the version appear?" check passes,
// and the release publishes a body with nothing in it.
func TestChangelogSectionRejectsAnEmptySection(t *testing.T) {
	changelog := fixture(t, "# Changelog\n\n## [0.3.0] - 2026-10-01\n\n\n## [0.2.0] - 2026-09-01\n\n- Something.\n")

	stdout, stderr, exit := runScript(t, "v0.3.0", changelog)
	if exit != 1 {
		t.Fatalf("exit code = %d, want 1 (an empty section is no section)\nstdout:\n%s\nstderr:\n%s",
			exit, stdout, stderr)
	}
	if !strings.Contains(stderr, "has no section for v0.3.0") {
		t.Errorf("stderr does not explain the failure:\n%s", stderr)
	}
}

// TestChangelogSectionUsageErrors covers the arguments a caller can get wrong. The
// distinction that matters is exit 2 (the invocation or the file is unusable)
// against exit 1 (the release genuinely has no notes) — CI reports the two
// differently, because only one of them is fixed by writing notes.
func TestChangelogSectionUsageErrors(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantExit int
	}{
		{"no arguments", nil, 2},
		{"too many arguments", []string{"v0.1.0", "a", "b"}, 2},
		{"a changelog that is not there", []string{"v0.1.0", filepath.Join(t.TempDir(), "absent.md")}, 2},
		{"help", []string{"--help"}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, exit := runScript(t, tc.args...)
			if exit != tc.wantExit {
				t.Errorf("exit code = %d, want %d", exit, tc.wantExit)
			}
		})
	}
}

//
// CHANGELOG.md is shaped the way the gate reads it
//

var (
	// A Keep a Changelog release heading: `## [X.Y.Z] - YYYY-MM-DD`.
	releaseHeading = regexp.MustCompile(
		`(?m)^## \[([0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?)\] - (\d{4}-\d{2}-\d{2})\s*$`)
	// Any level-two heading, so an unrecognised one can be reported rather than
	// silently ignored.
	anyH2 = regexp.MustCompile(`(?m)^## (.*)$`)
	// The `VERSION ?= ` line in the Makefile, which is what the artifacts pin.
	makefileVersion = regexp.MustCompile(`(?m)^VERSION \?= (\S+)\s*$`)
)

// committedVersion is the version the tree is currently prepared to release.
func committedVersion(t *testing.T) string {
	t.Helper()
	m := makefileVersion.FindStringSubmatch(readFile(t, "Makefile"))
	if m == nil {
		t.Fatal("no `VERSION ?= ` line in the Makefile")
	}
	return m[1]
}

// TestChangelogIsKeepAChangelog checks the structure the extractor depends on.
// Prose is free to change; the heading grammar is not, because it is parsed.
func TestChangelogIsKeepAChangelog(t *testing.T) {
	changelog := readFile(t, "CHANGELOG.md")

	if !strings.Contains(changelog, "keepachangelog.com") {
		t.Error("CHANGELOG.md no longer names the format it follows; the heading grammar is parsed, " +
			"so a reader adding a section needs to know which grammar")
	}
	if !strings.Contains(changelog, "## [Unreleased]") {
		t.Error("CHANGELOG.md has no `## [Unreleased]` section; work merged between releases has nowhere to go")
	}

	// Every level-two heading is either Unreleased or a dated release. A heading
	// the extractor cannot parse is invisible to it, which is how a release ends
	// up with no notes despite somebody having written them.
	for _, m := range anyH2.FindAllStringSubmatch(changelog, -1) {
		heading := "## " + m[1]
		if strings.TrimSpace(m[1]) == "[Unreleased]" {
			continue
		}
		if !releaseHeading.MatchString(heading) {
			t.Errorf("%q is neither `## [Unreleased]` nor `## [X.Y.Z] - YYYY-MM-DD`; "+
				"hack/changelog-section.sh cannot find it", heading)
		}
	}

	versions := releaseHeading.FindAllStringSubmatch(changelog, -1)
	if len(versions) == 0 {
		t.Fatal("CHANGELOG.md contains no released version section")
	}

	// Newest first, which is both the convention and what makes the top of the
	// file the answer to "what is the latest release?".
	for i := 1; i < len(versions); i++ {
		if compareSemver(versions[i-1][1], versions[i][1]) <= 0 {
			t.Errorf("version sections are not in descending order: [%s] precedes [%s]",
				versions[i-1][1], versions[i][1])
		}
	}

	// Keep a Changelog's own groups, and only those. An invented group name is not
	// wrong so much as unnoticed: a reader scanning for "Removed" will not find
	// breakage filed under "Notes", and in a v0.x project that is the one thing
	// they must not miss.
	allowedGroups := map[string]bool{
		"Added": true, "Changed": true, "Deprecated": true,
		"Removed": true, "Fixed": true, "Security": true,
		// Not a Keep a Changelog group, and kept deliberately: upgrade steps are
		// instructions rather than a list of changes, and burying them inside
		// `Changed` is how people miss them.
		"Migration": true,
	}
	for _, m := range regexp.MustCompile(`(?m)^### (.*)$`).FindAllStringSubmatch(changelog, -1) {
		// A group may carry a trailing clause ("Removed — BREAKING: …"), so only the
		// first word is checked.
		group := strings.Fields(m[1])
		if len(group) == 0 {
			t.Errorf("empty `### ` heading in CHANGELOG.md")
			continue
		}
		if !allowedGroups[group[0]] {
			t.Errorf("`### %s` is not a Keep a Changelog group; breakage filed under an invented "+
				"heading is breakage a reader scanning for `Removed` will miss", m[1])
		}
	}
}

// TestChangelogHasASectionForTheCommittedVersion is the check that would have
// caught the release failing at tag time: whatever VERSION says the tree is ready
// to release must already have notes.
func TestChangelogHasASectionForTheCommittedVersion(t *testing.T) {
	version := committedVersion(t)

	stdout, stderr, exit := runScript(t, "v"+version)
	if exit != 0 {
		t.Fatalf("CHANGELOG.md has no usable section for the committed VERSION %s, so tagging it would "+
			"fail the release gate\nstderr:\n%s", version, stderr)
	}
	if len(strings.TrimSpace(stdout)) < 100 {
		t.Errorf("the section for %s is %d characters; that is not release notes", version, len(stdout))
	}
}

// TestChangelogVersionsHaveLinkDefinitions keeps the compare links complete.
// They are the only navigational aid in a file that grows monotonically, and a
// missing one renders as literal `[0.2.0]` text.
func TestChangelogVersionsHaveLinkDefinitions(t *testing.T) {
	changelog := readFile(t, "CHANGELOG.md")

	defined := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^\[([^\]]+)\]:\s+\S+`).FindAllStringSubmatch(changelog, -1) {
		defined[m[1]] = true
	}
	if !defined["Unreleased"] {
		t.Error("no link definition for [Unreleased]")
	}
	for _, m := range releaseHeading.FindAllStringSubmatch(changelog, -1) {
		if !defined[m[1]] {
			t.Errorf("no link definition for [%s]; the heading renders as literal text", m[1])
		}
	}

	// The extractor treats a link-reference definition as the end of a section, so
	// that the compare-link block at the bottom of the file — which sits inside the
	// oldest section by position and belongs to none of them by meaning — does not
	// end up pasted into a GitHub Release body as literal text. The price of that
	// is that section bodies must use inline links, and this is where the price is
	// enforced: a reference-style link inside a section would silently truncate that
	// release's notes at the point it appeared.
	for label := range defined {
		if label == "Unreleased" {
			continue
		}
		if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$`).MatchString(label) {
			t.Errorf("[%s] is a reference-style link label rather than a version; "+
				"hack/changelog-section.sh would truncate that release's notes where it appears. "+
				"Use an inline [text](target) link instead.", label)
		}
	}
}

// compareSemver orders two X.Y.Z(-prerelease) strings. It is not a general semver
// implementation — it exists to check that the sections in one file descend, and a
// prerelease sorting before its own release is enough precision for that.
func compareSemver(a, b string) int {
	aCore, aPre, _ := strings.Cut(a, "-")
	bCore, bPre, _ := strings.Cut(b, "-")

	aParts, bParts := strings.Split(aCore, "."), strings.Split(bCore, ".")
	for i := range 3 {
		var ai, bi int
		if _, err := fmt.Sscanf(aParts[i], "%d", &ai); err != nil {
			return strings.Compare(a, b)
		}
		if _, err := fmt.Sscanf(bParts[i], "%d", &bi); err != nil {
			return strings.Compare(a, b)
		}
		if ai != bi {
			if ai > bi {
				return 1
			}
			return -1
		}
	}
	switch {
	case aPre == bPre:
		return 0
	case aPre == "": // a release outranks its own prereleases
		return 1
	case bPre == "":
		return -1
	default:
		return strings.Compare(aPre, bPre)
	}
}

//
// The release path is wired up
//

// TestReleaseTargetsExist checks that the targets the workflow calls are the
// targets the Makefile defines. This seam breaks silently: renaming a target
// leaves CI green until somebody tags, and then the release fails at the moment it
// is least convenient.
func TestReleaseTargetsExist(t *testing.T) {
	makefile := readFile(t, "Makefile")

	for _, target := range []string{
		"release-verify-version:", "release-notes:", "release-artifacts:",
		"release-image:", "release-dry-run:",
	} {
		if !strings.Contains(makefile, "\n"+target) {
			t.Errorf("the Makefile no longer defines %s", strings.TrimSuffix(target, ":"))
		}
	}

	// BUILDX_OUTPUT is what makes a rehearsal possible at all: without it the only
	// way to exercise the multi-arch build is to push somewhere.
	if !strings.Contains(makefile, "BUILDX_OUTPUT ?= --push") {
		t.Error("the Makefile no longer defaults BUILDX_OUTPUT to --push; a release would build without pushing")
	}
	// The buildx build must not be prefixed with `-`. It was, once, and make
	// ignored the exit code: a registry that refused the push reported success.
	if regexp.MustCompile(`(?m)^\s+-\s+\$\(CONTAINER_TOOL\) buildx build`).MatchString(makefile) {
		t.Error("the buildx build line is prefixed with `-`, so make would ignore a failed build or push")
	}
	if !strings.Contains(makefile, "IMAGE_REPO ?= ") {
		t.Error("the Makefile no longer defines IMAGE_REPO; the registry would be named in more than one place")
	}
}

// TestReleaseScriptIsExecutable guards the same class of mistake the quickstart
// checks guard: the Makefile runs the script directly.
func TestReleaseScriptIsExecutable(t *testing.T) {
	info, err := os.Stat(repoPath(changelogScript))
	if err != nil {
		t.Fatalf("stat %s: %v", changelogScript, err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Errorf("%s is not executable; `make release-notes` runs it directly", changelogScript)
	}
}

// TestReleaseWorkflow checks the workflow's shape rather than its wording: which
// tags start a release, that the gate runs before anything is published, and that
// each job calls the make target it is supposed to.
func TestReleaseWorkflow(t *testing.T) {
	workflow := readFile(t, ".github/workflows/release.yml")

	tests := []struct {
		name string
		want string
		why  string
	}{
		{"tag trigger", "v[0-9]+.[0-9]+.[0-9]+", "a release is a tag, and only a version-shaped one"},
		{"prerelease trigger", "v[0-9]+.[0-9]+.[0-9]+-*", "release candidates are how a release is rehearsed for real"},
		{"dry-run trigger", "workflow_dispatch", "a rehearsal that cannot be run in CI is not a rehearsal of CI"},
		{"version gate", "make release-verify-version", "a tag that disagrees with the tree must not publish"},
		{"changelog gate", "make release-notes", "a tag with no notes must not publish"},
		{"image build", "make release-image", "the multi-arch build reuses the repository's buildx target"},
		{"artifacts", "make release-artifacts", "install.yaml, the chart and the checksums"},
		{"notes are the body", "--notes-file dist/release/RELEASE_NOTES.md", "the changelog is the release notes"},
		{"checksums attached", "dist/release/checksums.txt", "checksums are published, not merely computed"},
		{"sbom attached", "dist/release/*-sbom.spdx.json", "an SBOM nobody can download describes nothing"},
		{"the image is signed", "make release-sign", "an unsigned release is the thing Task 7.4 exists to end"},
		{"the image is described", "make release-sbom", "the SBOM is generated from the pushed image, by digest"},
		{"provenance is generated", "actions/attest-build-provenance", "SLSA provenance for the image and the assets"},
		{"the release verifies itself", "make release-verify", "a signature that does not verify must fail the release"},
		{"chart attached", "dist/release/kuberecord-*.tgz", "the packaged chart is an artifact of the release"},
		{"gate precedes publishing", "needs: [gate, image]", "publishing is not undoable, so ordering is the safeguard"},
		{"image needs the gate", "needs: gate", "an image pushed under a rejected tag cannot be unpushed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(workflow, tc.want) {
				t.Errorf("release.yml no longer contains %q — %s", tc.want, tc.why)
			}
		})
	}

	// Actions are pinned by commit SHA throughout this repository; a floating tag
	// in the one workflow that has `contents: write` is the worst place to start.
	for _, m := range regexp.MustCompile(`(?m)^\s+uses: (\S+)`).FindAllStringSubmatch(workflow, -1) {
		if !regexp.MustCompile(`@[0-9a-f]{40}$`).MatchString(m[1]) {
			t.Errorf("%q is not pinned to a commit SHA", m[1])
		}
	}
}

// TestVersioningPolicyIsDocumented asserts that the three version axes are all
// stated. The policy is the acceptance criterion here: an operator whose minors
// may break, CRDs at v1alpha1, and a frozen schema v1 are three different
// promises, and a reader who conflates any two of them will draw the wrong
// conclusion about whether an upgrade is safe.
func TestVersioningPolicyIsDocumented(t *testing.T) {
	policy := readFile(t, "docs/RELEASING.md")

	tests := []struct {
		name string
		want string
	}{
		{"operator semver is pre-1.0", "pre-1.0"},
		{"a minor may break", "minor bump may break"},
		{"the CRD API version", "v1alpha1"},
		{"the schema version is frozen", "frozen"},
		{"schema v1", "`v1`"},
		{"no floating latest tag", "no floating `latest`"},
		{"checksums", "checksums.txt"},
		{"the dry run", "make release-dry-run"},
		// A release publishes evidence as well as artifacts (Task 7.4), and the page
		// that lists what a tag publishes is where a reader finds out it exists at
		// all. Verification itself lives in docs/VERIFYING.md.
		{"the image signature", "cosign"},
		{"the SBOM", "SBOM"},
		{"provenance", "provenance"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(policy, tc.want) {
				t.Errorf("docs/RELEASING.md no longer states %s (looked for %q)", tc.name, tc.want)
			}
		})
	}

	// The policy is only useful if a reader finds it.
	if !strings.Contains(readFile(t, "README.md"), "docs/RELEASING.md") {
		t.Error("README.md does not link docs/RELEASING.md")
	}
	if !strings.Contains(readFile(t, "CHANGELOG.md"), "docs/RELEASING.md") {
		t.Error("CHANGELOG.md does not link docs/RELEASING.md, where what a version number promises is written down")
	}
}

// TestReleaseArtifactsAreIgnored keeps generated per-tag artifacts out of the
// tree. dist/install.yaml is committed on purpose; dist/release/ is built from it
// for one tag and would otherwise show up as untracked noise in every release
// rehearsal — the state in which somebody is most likely to `git add -A`.
func TestReleaseArtifactsAreIgnored(t *testing.T) {
	if !strings.Contains(readFile(t, ".gitignore"), "dist/release/") {
		t.Error(".gitignore does not ignore dist/release/, which `make release-artifacts` fills")
	}
}

//
// The supply chain is verifiable (Task 7.4)
//

// releaseWorkflow is the part of release.yml these tests reason about
// structurally rather than by substring. Permissions are the reason: a grant is
// scoped by where it appears, and "the file does not contain id-token: write" and
// "no job that should not have it has it" are very different assertions. The first
// is what a grep can say, and it would fail the moment the feature was added
// correctly.
//
// Comments are lost by the parse, so the SHA-pinning check stays on the raw text.
type releaseWorkflow struct {
	// A pointer so an absent block is distinguishable from `permissions: {}`,
	// which is the whole point of the top-level one.
	Permissions *map[string]string `json:"permissions"`
	Jobs        map[string]struct {
		Permissions *map[string]string `json:"permissions"`
		Steps       []struct {
			Name string `json:"name"`
			Uses string `json:"uses"`
			If   string `json:"if"`
			Run  string `json:"run"`
		} `json:"steps"`
	} `json:"jobs"`
}

func parseReleaseWorkflow(t *testing.T) releaseWorkflow {
	t.Helper()
	var wf releaseWorkflow
	if err := yaml.Unmarshal([]byte(readFile(t, releaseWorkflowPath)), &wf); err != nil {
		t.Fatalf("parse %s: %v", releaseWorkflowPath, err)
	}
	if len(wf.Jobs) == 0 {
		t.Fatalf("%s parsed with no jobs", releaseWorkflowPath)
	}
	return wf
}

const releaseWorkflowPath = ".github/workflows/release.yml"

// oidcJobs are the jobs allowed to mint an OIDC token, and why each one needs to.
//
// Keyless signing and attestation both need `id-token: write`, and the two halves
// of a release need it in two places: a signature is about a digest, which only
// exists after the push, and an artifact attestation is about files that only
// exist where they were built. Splitting it this way keeps each job unable to do
// the other's damage — the image job cannot publish a Release and the publish job
// cannot push an image — which is the property this test exists to preserve.
var oidcJobs = map[string]string{
	"image":   "signs the image and attests it, and the digest exists nowhere earlier",
	"publish": "attests the artifacts it just built, which exist nowhere earlier",
}

// TestReleaseWorkflowPermissions is the deny-by-default posture, asserted rather
// than assumed. The failure it guards against is silent and serious: a
// `permissions:` block that drifts up to the workflow level hands every job —
// including the gate, which runs before anything is published — the ability to
// mint a signing identity for this repository.
func TestReleaseWorkflowPermissions(t *testing.T) {
	wf := parseReleaseWorkflow(t)

	if wf.Permissions == nil {
		t.Fatalf("%s has no top-level `permissions:` block; without one every job "+
			"inherits the repository default", releaseWorkflowPath)
	}
	if len(*wf.Permissions) != 0 {
		t.Errorf("%s grants %v at the workflow level; it must be `permissions: {}` so "+
			"each job states what it needs", releaseWorkflowPath, *wf.Permissions)
	}

	for name, job := range wf.Jobs {
		t.Run(name, func(t *testing.T) {
			if job.Permissions == nil {
				t.Fatalf("job %q declares no permissions, so it inherits none and cannot "+
					"even check out the code", name)
			}
			perms := *job.Permissions

			_, mayHaveOIDC := oidcJobs[name]
			for _, scope := range []string{"id-token", "attestations"} {
				if perms[scope] == "" {
					continue
				}
				if !mayHaveOIDC {
					t.Errorf("job %q is granted %s: %s. Only %v need it, and widening that "+
						"set is a decision to make on purpose", name, scope, perms[scope],
						sortedKeys(oidcJobs))
				}
			}
			if mayHaveOIDC && perms["id-token"] != "write" {
				t.Errorf("job %q no longer has id-token: write, so it cannot sign or attest "+
					"(it %s)", name, oidcJobs[name])
			}
			if mayHaveOIDC && perms["attestations"] != "write" {
				t.Errorf("job %q no longer has attestations: write, so provenance cannot be "+
					"recorded", name)
			}
		})
	}

	// The gate is the job whose whole purpose is to run before anything can be
	// published. It reads three files; anything more than read access to the
	// repository is a mistake.
	gate, ok := wf.Jobs["gate"]
	if !ok {
		t.Fatal("release.yml no longer has a gate job")
	}
	if want := map[string]string{"contents": "read"}; !mapsEqual(*gate.Permissions, want) {
		t.Errorf("the gate job is granted %v, not %v. It decides whether a release happens; "+
			"it must not be able to make one", *gate.Permissions, want)
	}
}

// TestPublishingStepsAreGatedOnTheDryRun is the check that keeps a rehearsal a
// rehearsal. Signing writes to a registry and to a public transparency log,
// attesting writes a record against the repository, and creating a Release is
// visible to everyone immediately: all three are publications, and a
// `workflow_dispatch` run that performed any of them would be publishing under the
// name of a rehearsal.
func TestPublishingStepsAreGatedOnTheDryRun(t *testing.T) {
	wf := parseReleaseWorkflow(t)

	const gate = "env.DRY_RUN == 'false'"
	publishing := []struct {
		match func(name, uses, run string) bool
		what  string
	}{
		{
			func(_, uses, _ string) bool { return strings.Contains(uses, "attest-build-provenance") },
			"an attestation is a record written against this repository",
		},
		{
			func(_, _, run string) bool { return strings.Contains(run, "make release-sign") },
			"a signature is a registry write and a public transparency-log entry",
		},
		{
			func(_, _, run string) bool { return strings.Contains(run, "gh release create") },
			"a published Release cannot be unpublished",
		},
		{
			func(_, _, run string) bool { return strings.Contains(run, "docker login") },
			"a rehearsal that authenticates to the registry could push",
		},
	}

	found := make([]int, len(publishing))
	for jobName, job := range wf.Jobs {
		for _, step := range job.Steps {
			for i, p := range publishing {
				if !p.match(step.Name, step.Uses, step.Run) {
					continue
				}
				found[i]++
				if step.If != gate {
					t.Errorf("%s/%q runs on a rehearsal (`if: %s`), but %s",
						jobName, step.Name, step.If, p.what)
				}
			}
		}
	}
	for i, n := range found {
		if n == 0 {
			t.Errorf("no step matched the %q case; either the step was renamed or the "+
				"release no longer does it", publishing[i].what)
		}
	}
}

// TestEveryWorkflowPinsActionsBySHA widens the existing release-only check to the
// whole directory. Task 7.4 adds actions in one workflow, but the convention is
// repository-wide, and a floating tag is a third party deciding what runs with
// this repository's tokens — which is the specific risk the rest of this task is
// about.
func TestEveryWorkflowPinsActionsBySHA(t *testing.T) {
	dir := repoPath(".github", "workflows")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	sha := regexp.MustCompile(`@[0-9a-f]{40}$`)
	uses := regexp.MustCompile(`(?m)^\s+-?\s*uses: (\S+)`)
	seen := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yml") {
			continue
		}
		content := readFile(t, filepath.ToSlash(filepath.Join(".github", "workflows", e.Name())))
		for _, m := range uses.FindAllStringSubmatch(content, -1) {
			seen++
			// A local action (./.github/actions/…) has no SHA to pin.
			if strings.HasPrefix(m[1], "./") {
				continue
			}
			if !sha.MatchString(m[1]) {
				t.Errorf("%s uses %q, which is not pinned to a commit SHA", e.Name(), m[1])
			}
		}
	}
	if seen == 0 {
		t.Error("found no `uses:` lines at all; the regexp stopped matching")
	}
}

// TestSupplyChainTargetsExist is TestReleaseTargetsExist's counterpart for the
// targets Task 7.4 adds. They break the same way: renaming one leaves CI green
// until somebody tags.
func TestSupplyChainTargetsExist(t *testing.T) {
	makefile := readFile(t, "Makefile")

	for _, target := range []string{
		"release-image-digest:", "release-sbom:", "release-sbom-local:",
		"release-sign:", "release-verify:", "release-checksums:",
		"release-rehearse-publishing:",
	} {
		if !strings.Contains(makefile, "\n"+target) {
			t.Errorf("the Makefile no longer defines %s", strings.TrimSuffix(target, ":"))
		}
	}

	// A signature or an attestation names a digest, never a tag: a tag can be
	// moved to a different image afterwards, which would leave a valid signature
	// describing something nobody released.
	if !strings.Contains(makefile, `"$(COSIGN)" sign --yes --recursive "$(IMAGE_REPO)@$(release-image-subject)"`) {
		t.Error("release-sign no longer signs IMAGE_REPO@<digest> recursively; signing a tag, " +
			"or signing only the manifest list, are both weaker than what is documented")
	}
	// The SBOM validation is the part that makes the artifact worth attaching.
	for _, want := range []string{"SBOM_MIN_PACKAGES", "spdxVersion"} {
		if !strings.Contains(makefile, want) {
			t.Errorf("release-sbom no longer checks %s; syft succeeds on an image whose "+
				"binary it could not read, and the result is a valid, useless document", want)
		}
	}
}

// verifyingPageClaims is what "Verifying a release" has to keep saying.
//
// Every entry is a command a reader copies or a limit they would otherwise get
// wrong. The two identity flags are here because a `cosign verify` without them
// answers "is this signed by anybody?", which any attacker who can obtain a
// Sigstore certificate can satisfy — a verification command that is weaker than it
// looks is worse than none.
var verifyingPageClaims = []struct {
	want string
	why  string
}{
	{"cosign verify", "the command the whole page exists to publish"},
	{"--certificate-oidc-issuer https://token.actions.githubusercontent.com", "who vouched for the signing identity"},
	{"--certificate-identity", "which workflow, on which ref, in which repository"},
	{"gh attestation verify", "the provenance half, published by the action that generates it"},
	{"--signer-workflow", "provenance from any workflow in the repository is a weaker claim"},
	{"sha256sum -c checksums.txt", "the existing check, still the fastest one"},
	{"spdx", "the SBOM's format, so a reader knows what they are holding"},
	{"does not sign", "a signed operator is not a signed audit trail"},
	{"transparency log", "keyless signing trades a key for two public services"},
	{"rehearsal", "a green rehearsal cannot prove a signature verifies"},
}

// TestVerifyingPageCoversItsSubject is the positive half of the pair: a page that
// listed no commands, or listed them without the flags that make them mean
// something, would satisfy every structural check in this file while being useless
// to the one audience it is for.
func TestVerifyingPageCoversItsSubject(t *testing.T) {
	page := readFile(t, "docs/VERIFYING.md")
	for _, tc := range verifyingPageClaims {
		t.Run(tc.want, func(t *testing.T) {
			if !strings.Contains(strings.ToLower(page), strings.ToLower(tc.want)) {
				t.Errorf("docs/VERIFYING.md no longer says %q — %s", tc.want, tc.why)
			}
		})
	}

	// A reader has to be able to find it from where the claim is made.
	if !strings.Contains(readFile(t, "README.md"), "(docs/VERIFYING.md)") {
		t.Error("README.md no longer links docs/VERIFYING.md, and the README is where a " +
			"stranger is told to install a tag")
	}
	if !strings.Contains(readFile(t, "docs/RELEASING.md"), "VERIFYING.md") {
		t.Error("docs/RELEASING.md no longer links docs/VERIFYING.md; what a release " +
			"publishes and how to check it are two halves of one story")
	}
	// The honest limit, cross-linked rather than restated, so the two pages cannot
	// drift into disagreeing about what kuberecord signs.
	if !strings.Contains(page, "RETENTION.md#kuberecord-does-not-sign-anything") {
		t.Error("docs/VERIFYING.md no longer links the retention page's limits section. " +
			"A reader who has just verified a signature is exactly the reader most likely " +
			"to believe the audit trail is signed too")
	}
}

// TestDocumentedSigningIdentityAgreesWithTheMakefile is the anti-drift check
// between prose and machinery, and it is the one seam here that nothing else would
// catch: the documented `cosign verify` command is a string a reader copies, and if
// it names a workflow file that has been renamed, verification fails for everyone
// while every test still passes.
func TestDocumentedSigningIdentityAgreesWithTheMakefile(t *testing.T) {
	makefile := readFile(t, "Makefile")

	// The Makefile builds the identity from the module path and one workflow path.
	// Both halves have to be true of the tree they are read from.
	if !strings.Contains(makefile, `GITHUB_REPO ?= $(shell sed -n 's|^module github.com/||p' go.mod)`) {
		t.Error("GITHUB_REPO is no longer derived from the module path, so the signing " +
			"identity and the repository can now disagree")
	}
	if !strings.Contains(makefile, "RELEASE_WORKFLOW ?= "+releaseWorkflowPath) {
		t.Errorf("the Makefile's RELEASE_WORKFLOW no longer names %s", releaseWorkflowPath)
	}
	if !strings.Contains(makefile, "COSIGN_ISSUER ?= https://token.actions.githubusercontent.com") {
		t.Error("COSIGN_ISSUER is no longer GitHub's Actions OIDC provider")
	}

	module := regexp.MustCompile(`(?m)^module github\.com/(\S+)`).
		FindStringSubmatch(readFile(t, "go.mod"))
	if module == nil {
		t.Fatal("go.mod has no github.com module path")
	}
	identityPrefix := "https://github.com/" + module[1] + "/" + releaseWorkflowPath + "@refs/tags/"

	// Every page that publishes the command has to publish the same one.
	for _, page := range []string{"docs/VERIFYING.md", "README.md"} {
		if !strings.Contains(readFile(t, page), identityPrefix) {
			t.Errorf("%s does not document the signing identity %q. That string is what a "+
				"verifier pins; if it is wrong, every copy-pasted verification fails",
				page, identityPrefix+"vX.Y.Z")
		}
	}

	// And the file it names has to exist, which is the half that would rot.
	if _, err := os.Stat(repoPath(releaseWorkflowPath)); err != nil {
		t.Errorf("the documented identity names %s, which does not exist: %v",
			releaseWorkflowPath, err)
	}
}

// sortedKeys is for error messages: a map iterated for a test failure should not
// print in a different order on every run.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// TestNoUnreliableDigestTemplate is a forbidden-token scan, and it exists because
// the tempting simplification here is a silent one.
//
// `docker buildx imagetools inspect --format '{{.Manifest.Digest}}'` reads like the
// obvious way to resolve a digest, and this repository documented it. On Docker
// Desktop's buildx v0.22 a template referencing `.Manifest` is ignored: the default
// human-readable listing is printed instead and the exit code is 0. Anything that
// captured that output would go on to sign, attest or pin whatever the text parsed
// as. Hashing the raw manifest bytes is what a digest is, and it behaves the same
// for one manifest and for an index.
//
// The scan is on *executable* occurrences only — recipe lines, workflow steps, and
// fenced code blocks in the documentation — because the three places that had to
// change also have to explain what they are avoiding, and a page that could not
// name the wrong command could not warn anybody off it. That is the same
// paired-guard shape the retired Object Lock claims use in test/docs.
func TestNoUnreliableDigestTemplate(t *testing.T) {
	const banned = "{{.Manifest.Digest}}"

	files := []string{"Makefile"}
	for _, dir := range []string{"docs", ".github/workflows"} {
		entries, err := os.ReadDir(repoPath(dir))
		if err != nil {
			t.Fatalf("read %s: %v", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || (!strings.HasSuffix(e.Name(), ".md") && !strings.HasSuffix(e.Name(), ".yml")) {
				continue
			}
			files = append(files, filepath.ToSlash(filepath.Join(dir, e.Name())))
		}
	}

	for _, f := range files {
		markdown := strings.HasSuffix(f, ".md")
		inFence := false
		for i, line := range strings.Split(readFile(t, f), "\n") {
			trimmed := strings.TrimLeft(line, " \t")
			if markdown && strings.HasPrefix(trimmed, "```") {
				inFence = !inFence
				continue
			}
			if !strings.Contains(line, banned) {
				continue
			}
			// Prose may name it; a comment explaining why it is not used may too.
			if markdown && !inFence {
				continue
			}
			if !markdown && (strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "@#")) {
				continue
			}
			t.Errorf("%s:%d resolves a digest with %s. Some buildx builds ignore that "+
				"template and print the default listing with exit code 0, so the caller gets "+
				"prose where it expected a digest. Hash the raw manifest instead: "+
				"`imagetools inspect --raw <ref> | sha256sum`.", f, i+1, banned)
		}
	}
}
