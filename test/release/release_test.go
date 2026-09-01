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
	"crypto/sha256"
	"encoding/hex"
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
	// Every hack/ script a release target invokes directly. A missing +x is
	// invisible in review — the diff shows the file, not the mode — and shows up
	// as "Permission denied" in the middle of a tag build.
	for script, why := range map[string]string{
		changelogScript: "`make release-notes` runs it directly",
		krewScript:      "`make release-krew-manifest` runs it directly",
		brewScript:      "`make release-brew-formula` runs it directly",
		digestsScript:   "`make release-krew-verify` runs it directly",
	} {
		info, err := os.Stat(repoPath(script))
		if err != nil {
			t.Fatalf("stat %s: %v", script, err)
		}
		if info.Mode().Perm()&0o111 == 0 {
			t.Errorf("%s is not executable; %s", script, why)
		}
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
		// Task 8.1. The chart is published to a registry as well as attached, so
		// that installing it does not depend on a GitHub redirect that a repository
		// name under the old account would destroy.
		{"the chart is pushed", "make release-chart-push", "the release-asset URL is the dependency this removes"},
		{"the chart is signed", "make release-chart-sign", "an unsigned chart is an unsigned operator by another route"},
		{"the chart verifies itself", "make release-chart-verify", "a signature that does not verify must fail the release"},
		{"the chart push is rehearsed", "make release-chart-rehearse", "a push is the step with something to get wrong"},
		// Task 12.1. The CLI ships from this run rather than from a pipeline of
		// its own, so each half of that has to still be wired up here.
		{"the CLI is described", "make release-cli-sbom", "a binary with a different dependency set needs its own SBOM"},
		{"the artifacts are signed", "make release-artifacts-sign",
			"an archive on a Release page has no digest, so the checksums carry the signature"},
		{"the signature verifies itself", "make release-artifacts-verify",
			"a signature that does not verify must fail the release"},
		{"the CLI archives are attached", "dist/release/kuberecord_*.tar.gz", "archives nobody can download install nothing"},
		{"the Windows archive is attached", "dist/release/kuberecord_*.zip", "krew's windows selector points at it"},
		{"the signature is attached", "dist/release/checksums.txt.sigstore.json",
			"a signature nobody can fetch verifies nothing"},
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
	gitignore := readFile(t, ".gitignore")
	if !strings.Contains(gitignore, "dist/release/") {
		t.Error(".gitignore does not ignore dist/release/, which `make release-artifacts` fills")
	}
	// dist/cli/ holds ten binaries of sixty megabytes each. A commit of one would
	// be noticed by whoever cloned next rather than by whoever made it.
	if !strings.Contains(gitignore, "dist/cli/") {
		t.Error(".gitignore does not ignore dist/cli/, which `make build-cli` fills")
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
		// Needs is a string or a list of them, depending on how the job spells
		// it, so it is read as `any` and compared as text. Only one test looks at
		// it, and what it asks is "does this ordering constraint mention that
		// job", which does not need the distinction.
		Needs       any                `json:"needs"`
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
	return parseWorkflow(t, releaseWorkflowPath)
}

// parseWorkflow is the same parse for any workflow in the directory.
//
// It exists because a `strings.Contains` over a workflow's raw text cannot tell a
// step from a comment about a step, and this file's comments deliberately quote
// the commands they explain — so a text search for `make build-cli` matches the
// paragraph saying why it runs even after the step running it is gone. That is
// not hypothetical: it happened to the assertion in TestCLICrossCompilesCgoFree.
func parseWorkflow(t *testing.T, path string) releaseWorkflow {
	t.Helper()
	var wf releaseWorkflow
	if err := yaml.Unmarshal([]byte(readFile(t, path)), &wf); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(wf.Jobs) == 0 {
		t.Fatalf("%s parsed with no jobs", path)
	}
	return wf
}

const releaseWorkflowPath = ".github/workflows/release.yml"

// permWrite is the only level any of the grants below may be held at. A scope
// present at `read` would satisfy a bare presence check while being unable to do
// the thing the job needs, which is the confusing half of a permissions bug.
const permWrite = "write"

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

// registryWriteJobs are the jobs allowed to write to the package registry, and
// why each one must.
//
// The publish job is here since Task 8.1, and its entry is the one worth reading
// twice, because it costs something. `helm package` stamps the current time into
// every tar header, so packaging a chart twice produces two archives with two
// digests; the only way the registry artifact and the `.tgz` on the Release page
// are the same bytes is for the job that packaged it to push it. The price is
// that the two publishing jobs are no longer symmetric — this one can now write
// to the registry. What survives, and what this map exists to keep, is that the
// image job still cannot publish a Release.
var registryWriteJobs = map[string]string{
	"image":   "pushes the multi-arch image, which is what a release is for",
	"publish": "pushes the chart archive it packaged, which no other job holds",
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

			if _, mayWrite := registryWriteJobs[name]; !mayWrite && perms["packages"] != "" {
				t.Errorf("job %q is granted packages: %s. Only %v push to the registry, and "+
					"widening that set is a decision to make on purpose", name, perms["packages"],
					sortedKeys(registryWriteJobs))
			}

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
			if mayHaveOIDC && perms["id-token"] != permWrite {
				t.Errorf("job %q no longer has id-token: write, so it cannot sign or attest "+
					"(it %s)", name, oidcJobs[name])
			}
			if mayHaveOIDC && perms["attestations"] != permWrite {
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
	for name, why := range registryWriteJobs {
		job, ok := wf.Jobs[name]
		if !ok {
			t.Errorf("release.yml no longer has a %q job, which %s", name, why)
			continue
		}
		if (*job.Permissions)["packages"] != permWrite {
			t.Errorf("job %q no longer has packages: write, so it cannot push (it %s)", name, why)
		}
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
		{
			func(_, _, run string) bool { return strings.Contains(run, "make release-chart-login") },
			"a rehearsal that authenticates to the chart registry could push",
		},
		{
			func(_, _, run string) bool { return strings.Contains(run, "make release-chart-push") },
			"a chart in a public registry cannot be unpublished",
		},
		{
			func(_, _, run string) bool { return strings.Contains(run, "make release-chart-sign") },
			"a signature is a registry write and a public transparency-log entry",
		},
		{
			func(_, _, run string) bool { return strings.Contains(run, "make release-artifacts-sign") },
			"sign-blob writes a public transparency-log entry, whatever it is over",
		},
		{
			func(_, _, run string) bool { return strings.Contains(run, "make release-brew-push") },
			"a commit pushed to the public Homebrew tap is what `brew install` serves",
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
		// Task 8.1's four, which break the same way.
		"release-chart-login:", "release-chart-push:", "release-chart-sign:",
		"release-chart-verify:", "release-chart-rehearse:",
		// Task 12.1's, likewise: rename one and CI stays green until somebody tags.
		"build-cli:", "verify-cli:", "release-cli:", "release-cli-sbom:",
		"release-artifacts-sign:", "release-artifacts-verify:",
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

	// The chart signature is over a digest for the same reason the image's is.
	if !strings.Contains(makefile, `"$(COSIGN)" sign --yes "$(CHART_OCI_REF)@$(release-chart-subject)"`) {
		t.Error("release-chart-sign no longer signs CHART_OCI_REF@<digest>; signing a tag is " +
			"weaker than what docs/VERIFYING.md documents, because a tag can be moved afterwards")
	}
}

// TestReleasePushesTheChartItPackaged is the one property in Task 8.1 that
// nothing else would catch, and it is invisible by inspection.
//
// `helm package` writes time.Now() into every tar header, so packaging the same
// unchanged chart twice produces two archives with two different sha256 sums. If
// the release ever repackages instead of pushing the archive release-artifacts
// already wrote, the registry artifact and the `.tgz` on the Release page become
// different bytes for one version — and checksums.txt, which is also the
// attestation's subject list, would then describe only one of them. Every claim
// this repository makes about verifying the chart depends on there being one
// answer.
func TestReleasePushesTheChartItPackaged(t *testing.T) {
	makefile := readFile(t, "Makefile")

	// The pushed file is the one release-artifacts produced, named from the same
	// variables rather than re-derived.
	if !strings.Contains(makefile, "RELEASE_CHART_TGZ = $(RELEASE_DIR)/$(CHART_RELEASE)-$(RELEASE_CHART_VERSION).tgz") {
		t.Error("RELEASE_CHART_TGZ no longer names the archive release-artifacts writes into " +
			"RELEASE_DIR, so the push and the release asset can now be different files")
	}

	push := makeRecipe(t, makefile, "release-chart-push")
	if strings.Contains(push, "helm) package") || strings.Contains(push, `HELM)" package`) {
		t.Error("release-chart-push packages the chart itself. It must push $(RELEASE_CHART_TGZ), " +
			"the archive release-artifacts already wrote: helm package stamps the current time " +
			"into every tar header, so a second packaging publishes different bytes under the " +
			"same version and checksums.txt stops covering one of them")
	}
	if !strings.Contains(push, "$(RELEASE_CHART_TGZ)") {
		t.Error("release-chart-push no longer pushes $(RELEASE_CHART_TGZ)")
	}

	// A stale digest from an earlier run is what release-chart-sign would read,
	// and it would sign an artifact from a previous release with nothing looking
	// wrong — the same reasoning that already removes the image's digest file.
	if !strings.Contains(makefile, `"$(RELEASE_CHART_DIGEST_FILE)"`) {
		t.Error("RELEASE_CHART_DIGEST_FILE is no longer referenced")
	}
	artifacts := makeRecipe(t, makefile, "release-artifacts")
	if !strings.Contains(artifacts, "$(RELEASE_CHART_DIGEST_FILE)") {
		t.Error("release-artifacts no longer removes the recorded chart digest. A digest left " +
			"behind by an earlier run is what release-chart-sign reads, so it would sign a " +
			"chart from a previous release")
	}
}

// TestChartLoginAuthenticatesCosignToo is the regression guard for a release that
// failed between two steps that both looked correct.
//
// `helm registry login` writes to helm's own registry configuration
// ($HELM_REGISTRY_CONFIG). cosign resolves registry credentials through
// go-containerregistry's default keychain, which reads the *Docker* configuration
// ($DOCKER_CONFIG/config.json) and has never looked at helm's. Authenticating only
// helm therefore produces one symptom and it is a misleading one: `helm push`
// succeeds, the digest is recorded, and the next command — `cosign sign`, which
// must POST the signature layer as a write to the same repository — fails with
// "UNAUTHORIZED: unauthenticated" against a registry the previous step
// demonstrably just wrote to. That is how v0.3.0's first attempt died.
//
// The image job never had the problem because it authenticates with `docker
// login`, which populates exactly the store cosign reads. This job has no image to
// build and so had no equivalent, and nothing noticed: the workflow_dispatch
// rehearsal pushes to a throwaway registry with no authentication at all, so the
// one path that would have caught it is the one path a rehearsal cannot exercise.
//
// Hence a test rather than a comment. The two logins have to stay together in the
// one target the workflow and the runbook both call, because a maintainer running
// the release by hand follows the same sequence and would hit the same wall.
func TestChartLoginAuthenticatesCosignToo(t *testing.T) {
	makefile := readFile(t, "Makefile")
	login := makeRecipe(t, makefile, "release-chart-login")

	if !strings.Contains(login, `"$(HELM)" registry login`) {
		t.Error("release-chart-login no longer logs helm in, so `helm push` cannot authenticate")
	}
	if !strings.Contains(login, `"$(COSIGN)" login`) {
		t.Error("release-chart-login no longer logs cosign in. `helm registry login` writes to " +
			"helm's own store and cosign reads the Docker one, so signing the chart it just " +
			"pushed would fail with UNAUTHORIZED against a registry helm had authenticated to")
	}

	// Both must be told about the same registry, from the same credentials. Two
	// logins that disagreed about the host would be worse than one login: the
	// failure would move to whichever command the drift did not cover.
	for _, want := range []string{
		`"$(HELM)" registry login "$(CHART_REGISTRY_HOST)"`,
		`"$(COSIGN)" login "$(CHART_REGISTRY_HOST)"`,
	} {
		if !strings.Contains(login, want) {
			t.Errorf("release-chart-login no longer runs %s; the two logins must name one host", want)
		}
	}
	if strings.Count(login, "--password-stdin") != 2 {
		t.Error("both logins must take the token on stdin. An argument is visible to every " +
			"process on the machine, and a release token that leaks is one that can push " +
			"over a published chart")
	}
	if strings.Contains(login, "--password ") || strings.Contains(login, "-p ") {
		t.Error("release-chart-login passes a password as an argument")
	}

	// The workflow must reach both through this target rather than acquiring a
	// second login of its own, which is how the two would drift apart again.
	workflow := readFile(t, releaseWorkflowPath)
	if !strings.Contains(workflow, "make release-chart-login") {
		t.Error("the release workflow no longer calls release-chart-login before pushing the chart")
	}

	// Ordering: the login has to come before the push, and the push before the
	// signature. A correct target called in the wrong order fails the same way.
	loginAt := strings.Index(workflow, "make release-chart-login")
	pushAt := strings.Index(workflow, "make release-chart-push")
	signAt := strings.Index(workflow, "make release-chart-sign")
	if loginAt < 0 || pushAt < 0 || signAt < 0 {
		t.Fatal("the release workflow no longer runs login, push and sign for the chart")
	}
	if loginAt >= pushAt || pushAt >= signAt {
		t.Error("the chart steps are out of order; login must precede the push and the push " +
			"must precede the signature over the digest it reports")
	}
}

// makeRecipe returns the recipe lines of one target: everything from the target
// line to the first line that is neither indented nor blank. Enough to ask what a
// single target does without parsing make.
func makeRecipe(t *testing.T, makefile, target string) string {
	t.Helper()
	lines := strings.Split(makefile, "\n")
	start := -1
	for i, line := range lines {
		if strings.HasPrefix(line, target+":") {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatalf("the Makefile no longer defines %s", target)
	}
	var recipe []string
	for _, line := range lines[start:] {
		if line != "" && !strings.HasPrefix(line, "\t") && !strings.HasPrefix(line, " ") {
			break
		}
		recipe = append(recipe, line)
	}
	return strings.Join(recipe, "\n")
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
	// The chart is a second signed artifact (Task 8.1), and the two ways of
	// getting it have to be stated as one thing, not two.
	{"ghcr.io/kuberecord/charts/kuberecord", "the chart's reference, which is the address and the whole address"},
	{"helm pull", "a reader who cannot fetch the chart cannot check it"},
	// The CLI is a third signed artifact (Task 12.1), and it is verified
	// differently — a file, not a registry reference — so its command has to be on
	// the page in full rather than left to be inferred from the image's.
	{"cosign verify-blob", "the archives are files, and a file is verified with a bundle rather than a digest"},
	{"checksums.txt.sigstore.json", "the bundle's name, which is the argument a reader passes"},
	{"kubectl-kuberecord", "both names ship in every archive, and which one is installed decides how it is invoked"},
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

// TestChartOCIReferenceAgreesWithTheMakefile is the anti-drift check for Task
// 8.1's published address, and it is the same class of failure as the signing
// identity's: the reference is a string a reader copies into `helm install`, and
// a page naming a registry path nothing was pushed to fails for everyone while
// every structural check still passes.
//
// The Makefile derives the chart's namespace from IMAGE_REPO so the registry is
// named once. This asserts the derivation still produces what the documentation
// publishes, and that the documentation publishes it everywhere it claims to.
func TestChartOCIReferenceAgreesWithTheMakefile(t *testing.T) {
	makefile := readFile(t, "Makefile")

	if !strings.Contains(makefile, "CHART_OCI_NAMESPACE ?= $(patsubst %/,%,$(dir $(IMAGE_REPO)))/charts") {
		t.Error("the chart's registry namespace is no longer derived from IMAGE_REPO, so the " +
			"registry can now be named in two places and a fork can publish under somebody " +
			"else's name")
	}

	// IMAGE_REPO is ghcr.io/<owner>/kuberecord; the chart lands beside it under
	// charts/, and helm push appends the chart's own name.
	image := regexp.MustCompile(`(?m)^IMAGE_REPO \?= (\S+)`).FindStringSubmatch(makefile)
	if image == nil {
		t.Fatal("the Makefile no longer defines IMAGE_REPO")
	}
	slash := strings.LastIndex(image[1], "/")
	if slash < 0 {
		t.Fatalf("IMAGE_REPO is %q, which names no registry namespace to put the chart beside", image[1])
	}
	wantRef := image[1][:slash] + "/charts/kuberecord"

	// Every page that tells a reader where the chart is has to say the same thing.
	for _, page := range []string{
		"README.md",
		"docs/VERIFYING.md",
		"docs/RELEASING.md",
		"deploy/charts/kuberecord/README.md",
	} {
		if !strings.Contains(readFile(t, page), wantRef) {
			t.Errorf("%s does not name %q. That string is what a reader passes to "+
				"`helm install`; if it is wrong, the install fails for everyone", page, wantRef)
		}
	}

	// The chart's tag is semver without the `v` while the signing identity keeps
	// it, which is the single easiest thing to get wrong about this reference.
	// Every page that shows the install has to warn about it, because a reader who
	// copies the `v` across gets `manifest unknown` and no explanation.
	// Any of these phrasings says it; the point is that the page says it at all.
	prefixWarnings := []string{"no `v`", "without a `v`", "drops the `v`"}
	for _, page := range []string{"README.md", "docs/VERIFYING.md", "deploy/charts/kuberecord/README.md"} {
		content := readFile(t, page)
		if !slices.ContainsFunc(prefixWarnings, func(w string) bool { return strings.Contains(content, w) }) {
			t.Errorf("%s shows the chart reference without warning that its tag drops the `v` "+
				"the operator tag and the signing identity both carry", page)
		}
	}
}

// TestOCIInstallPathIsSmoked keeps Task 8.1's acceptance criterion wired up: the
// OCI reference is a distribution channel, and the only way to know a chart
// installs from one is to install it from one.
func TestOCIInstallPathIsSmoked(t *testing.T) {
	makefile := readFile(t, "Makefile")
	for _, target := range []string{
		"test-e2e-helm-oci:", "deploy-e2e-helm-oci:", "undeploy-e2e-helm-oci:",
		"local-registry-up:", "local-registry-down:",
	} {
		if !strings.Contains(makefile, "\n"+target) {
			t.Errorf("the Makefile no longer defines %s", strings.TrimSuffix(target, ":"))
		}
	}

	// The registry the smoke and the rehearsal both push to decides what a chart
	// push is talking to. A floating tag there is a third party choosing that.
	if !regexp.MustCompile(`LOCAL_REGISTRY_IMAGE \?= \S+@sha256:[0-9a-f]{64}`).MatchString(makefile) {
		t.Error("LOCAL_REGISTRY_IMAGE is not pinned to a digest")
	}

	// The install must name the registry, not the directory that was packaged: an
	// install that read the chart off local disk would prove nothing about the
	// push it just performed.
	deploy := makeRecipe(t, makefile, "deploy-e2e-helm-oci")
	if !strings.Contains(deploy, "oci://$(LOCAL_REGISTRY_HOST)/charts/$(CHART_RELEASE)") {
		t.Error("deploy-e2e-helm-oci no longer installs from the OCI reference it pushed to, " +
			"so it tests packaging rather than distribution")
	}

	workflow := readFile(t, ".github/workflows/install-paths.yml")
	if !strings.Contains(workflow, "make test-e2e-helm-oci") {
		t.Error("install-paths.yml no longer runs the OCI install smoke, so the chart's " +
			"published distribution channel is untested until somebody tags")
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

//
// The CLI is built, packaged and signed by this pipeline (Task 12.1)
//

// cliPlatforms reads the platform list out of the Makefile, so every test below
// measures what the release actually builds rather than a list restated here.
//
// A test carrying its own copy would keep passing after a platform was dropped,
// which is the failure that matters: the archive simply stops being attached, and
// the person who finds out is whoever runs `krew install` on that platform.
func cliPlatforms(t *testing.T) []string {
	t.Helper()

	match := regexp.MustCompile(`(?m)^CLI_PLATFORMS \?= (.+)$`).FindStringSubmatch(readFile(t, "Makefile"))
	if match == nil {
		t.Fatal("the Makefile no longer defines CLI_PLATFORMS, so nothing decides what the CLI is built for")
	}
	platforms := strings.Fields(match[1])
	if len(platforms) == 0 {
		t.Fatal("CLI_PLATFORMS is empty; a release would attach no CLI archives and fail nowhere")
	}
	return platforms
}

// TestCLICrossCompilesCgoFree is the acceptance criterion that would regress
// silently, so it is asserted three ways: the build sets it, the build reads it
// back, and CI runs the build.
//
// The middle one is the point. `CGO_ENABLED=0` on a command line is a request; the
// Go toolchain records the value it actually used in the binary, and that is the
// only witness that survives an environment exporting CGO_ENABLED=1 or a
// dependency that quietly needs cgo on one platform. D18 makes this property
// load-bearing for krew: a cgo build is dynamically linked against the machine it
// was built on and cannot cross-compile at all.
func TestCLICrossCompilesCgoFree(t *testing.T) {
	makefile := readFile(t, "Makefile")

	if !strings.Contains(makefile, "CGO_ENABLED=0 GOOS=") {
		t.Error("build-cli no longer sets CGO_ENABLED=0 for the cross-compile; D18's " +
			"static build is what makes krew distribution five archives from one make")
	}
	// The read-back. `go version -m` is what prints the recorded build settings.
	if !strings.Contains(makefile, `go version -m "$$binary"`) {
		t.Error("verify-cli no longer reads the build settings back out of the binaries. " +
			"Trusting the command line asserts that the recipe was written correctly, " +
			"not that the artifact is what it claims")
	}
	if !strings.Contains(makefile, `grep -qE 'build[[:space:]]+CGO_ENABLED=0'`) {
		t.Error("verify-cli no longer matches CGO_ENABLED=0 in the recorded build settings, so " +
			"the read-back above is reading something other than the setting it exists for")
	}

	// And the third: a property nothing runs is a comment. Read out of the parsed
	// steps rather than the file's text, because the file's text also contains the
	// comment explaining the step.
	if !workflowRuns(t, ".github/workflows/test.yml", "make build-cli") {
		t.Error("no step in .github/workflows/test.yml runs `make build-cli`, so the cgo-free " +
			"cross-compile is asserted only at tag time — which is after the release it " +
			"would have stopped")
	}
}

// workflowRuns reports whether any step of any job runs a command containing want.
func workflowRuns(t *testing.T, path, want string) bool {
	t.Helper()
	for _, job := range parseWorkflow(t, path).Jobs {
		for _, step := range job.Steps {
			if strings.Contains(step.Run, want) {
				return true
			}
		}
	}
	return false
}

// TestCLIBuildsBothNamesForEveryPlatform is the other half of the build criterion.
//
// Both names come from one compilation rather than two builds of the same source,
// which is what makes them the same bytes: a plugin and a standalone binary that
// could differ would be two products with one version number.
func TestCLIBuildsBothNamesForEveryPlatform(t *testing.T) {
	makefile := readFile(t, "Makefile")

	for _, want := range []string{
		"CLI_PLUGIN_NAME ?= kubectl-kuberecord",
		"CLI_STANDALONE_NAME ?= kuberecord",
	} {
		if !strings.Contains(makefile, want) {
			t.Errorf("the Makefile no longer defines %q; kubectl finds a plugin by file name "+
				"alone, so one of these names is not a choice", want)
		}
	}
	// Copied, not compiled twice.
	if !strings.Contains(makefile, `cp "$$staged/$(CLI_PLUGIN_NAME)$$suffix" "$$staged/$(CLI_STANDALONE_NAME)$$suffix"`) {
		t.Error("build-cli no longer copies one compilation into the second name. Two builds " +
			"could differ — in flags, in the tree they saw, in the moment they ran — and " +
			"nothing downstream would notice")
	}

	// The five the acceptance criterion names. D18's prose says six; the task's
	// list is five and is what ships, so this pins the list that exists.
	want := []string{"linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64", "windows/amd64"}
	if got := cliPlatforms(t); !slices.Equal(got, want) {
		t.Errorf("CLI_PLATFORMS is %v, want %v. krew's plugin manifest has one entry per "+
			"platform here, so dropping one silently stops shipping to those users", got, want)
	}
}

// TestCLIPlatformsAreDocumented is the anti-drift check between what is built and
// what a reader is told is built.
//
// It is the same class as the signing identity's: prose nobody can run, describing
// machinery that moved. A reader deciding whether their architecture is supported
// reads the page, not the Makefile.
func TestCLIPlatformsAreDocumented(t *testing.T) {
	page := readFile(t, "docs/RELEASING.md")
	for _, platform := range cliPlatforms(t) {
		if !strings.Contains(page, platform) {
			t.Errorf("docs/RELEASING.md does not name %s, which the release builds a CLI "+
				"archive for", platform)
		}
	}
}

// TestCLIArchivesAreChecksummedAndAttached is the property that ties the archives
// into the evidence the rest of the release already carries.
//
// checksums.txt is the attestation's subject list and, since this task, the thing
// the signature is over. An archive missing from it is an archive with no
// provenance and no signature — published, and backed by nothing — and nothing
// else in the release would fail.
func TestCLIArchivesAreChecksummedAndAttached(t *testing.T) {
	makefile := readFile(t, "Makefile")

	// Required by name, on the same argument install.yaml is: a checksums file
	// that silently omits an asset is worse than none, because it looks complete.
	const required = `assets="install.yaml kuberecord-$(RELEASE_CHART_VERSION).tgz $(RELEASE_CLI_ARCHIVES)"`
	if !strings.Contains(makefile, required) {
		t.Error("release-checksums no longer requires the CLI archives by name. Made optional, " +
			"a release that failed to build them would publish a complete-looking checksums " +
			"file describing a release missing its CLI")
	}
	// And they have to exist before the first checksum run, which is why
	// release-artifacts builds them rather than a separate workflow step.
	artifacts := strings.Index(makefile, "release-artifacts: release-verify-version")
	if artifacts < 0 {
		t.Fatal("the Makefile no longer defines release-artifacts")
	}
	recipe := makefile[artifacts:]
	buildsCLI := strings.Index(recipe, "$(MAKE) release-cli")
	checksums := strings.Index(recipe, "$(MAKE) release-checksums")
	if buildsCLI < 0 {
		t.Fatal("release-artifacts no longer builds the CLI, so checksums.txt would require " +
			"archives nothing produced")
	}
	if checksums < 0 || buildsCLI > checksums {
		t.Error("release-artifacts computes the checksums before it builds the CLI archives, " +
			"so the first run would fail on assets that arrive afterwards")
	}
}

// TestArtifactSignatureCoversTheChecksums pins how the archives are signed, which
// is the one part of this task that had to differ from what the release already
// did.
//
// The image and the chart are signed by digest because a registry has one. A file
// on a Release page does not, so the subject is checksums.txt — and the
// consequence, which is worth a test rather than a comment, is that the signature
// covers every asset in that file. Signing an archive individually instead would
// leave install.yaml and the chart where they were.
func TestArtifactSignatureCoversTheChecksums(t *testing.T) {
	makefile := readFile(t, "Makefile")

	if !strings.Contains(makefile,
		`"$(COSIGN)" sign-blob --yes --bundle "$(RELEASE_CHECKSUMS_BUNDLE)" "$(RELEASE_CHECKSUMS)"`) {
		t.Error("release-artifacts-sign no longer signs checksums.txt into a bundle. A bundle " +
			"carries the certificate, the signature and the log proof together, which is what " +
			"lets a reader verify with nothing but the file and the bundle")
	}
	// Verification pins both halves of the identity, exactly as the image's does.
	// A `verify-blob` without them answers "is this signed by anybody?", which
	// anyone who can obtain a Sigstore certificate can satisfy.
	for _, want := range []string{
		`--certificate-oidc-issuer "$(COSIGN_ISSUER)"`,
		`--certificate-identity "$(COSIGN_IDENTITY)"`,
	} {
		if !strings.Contains(makefile, want) {
			t.Errorf("release-artifacts-verify does not pass %s, so it would accept a signature "+
				"from any workflow the issuer serves", want)
		}
	}
	// And the digests themselves. A valid signature over a list nothing was
	// checked against proves the list authentic and says nothing about the bytes.
	if !strings.Contains(makefile, "sha256sum -c checksums.txt") {
		t.Error("release-artifacts-verify no longer checks the assets against the list it just " +
			"verified the signature of")
	}
}

// TestTheRehearsalExercisesTheCLI is the acceptance criterion about
// `workflow_dispatch`, and the line it draws is the one the rest of this workflow
// already draws.
//
// A cross-compile publishes nothing, so a rehearsal does it for real — and the
// CLI's SBOM too, since that is scanned from a local binary rather than from
// something pushed. The signature is the only half that cannot be rehearsed,
// because `sign-blob` writes to a public transparency log, so it is printed
// instead.
func TestTheRehearsalExercisesTheCLI(t *testing.T) {
	wf := parseReleaseWorkflow(t)

	var buildsArtifacts, describesCLI bool
	for _, job := range wf.Jobs {
		for _, step := range job.Steps {
			// `release-artifacts` is what builds the archives, and it must not be
			// gated on the dry run: building them is the half of the CLI story a
			// rehearsal exists to exercise.
			if strings.Contains(step.Run, "make release-artifacts ") {
				buildsArtifacts = true
				if step.If != "" {
					t.Errorf("the artifacts (and with them the CLI archives) are built under "+
						"`if: %s`, so a rehearsal would not exercise the cross-compile", step.If)
				}
			}
			if strings.Contains(step.Run, "make release-cli-sbom") {
				describesCLI = true
				if step.If != "" {
					t.Errorf("the CLI SBOM is produced under `if: %s`, though it is scanned from "+
						"a local binary and needs nothing published", step.If)
				}
			}
		}
	}
	if !buildsArtifacts {
		t.Error("no step runs `make release-artifacts`, so nothing builds the CLI archives")
	}
	if !describesCLI {
		t.Error("no step runs `make release-cli-sbom`")
	}

	// The printed half. A rehearsal must say what it is deliberately not doing,
	// or the difference between it and a release is invisible in its log.
	makefile := readFile(t, "Makefile")
	if !strings.Contains(makefile, "release-artifacts-sign release-artifacts-verify \\") {
		t.Error("release-rehearse-publishing no longer prints the artifact signing commands, " +
			"so a rehearsal is silent about the one step it cannot perform")
	}
}

// TestCLISBOMIsValidated keeps the CLI's SBOM to the standard the image's is held
// to: syft succeeds on a file whose contents it could not read, and the result is
// a valid SPDX document describing nothing.
func TestCLISBOMIsValidated(t *testing.T) {
	makefile := readFile(t, "Makefile")

	start := strings.Index(makefile, "release-cli-sbom: ##")
	if start < 0 {
		t.Fatal("the Makefile no longer defines release-cli-sbom")
	}
	recipe := makefile[start:]
	if end := strings.Index(recipe, "\n.PHONY:"); end > 0 {
		recipe = recipe[:end]
	}

	for want, why := range map[string]string{
		`"spdxVersion"`:             "a document that is not SPDX is not the artifact that was promised",
		"SBOM_MIN_PACKAGES":         "a scan that read no modules produces a valid, useless document",
		"github.com/$(GITHUB_REPO)": "an SBOM that does not mention this module is describing something else",
	} {
		if !strings.Contains(recipe, want) {
			t.Errorf("release-cli-sbom no longer checks %s: %s", want, why)
		}
	}
}

//
// Distribution: krew and Homebrew (Task 12.2)
//

// The two generators and the extractor that reads their output back.
//
// They are shell scripts for the same reason hack/changelog-section.sh is: the
// release workflow calls make targets, a make target shelling out to a script is
// one fewer thing between a tag and an artifact, and the script is then testable
// from here exactly as the changelog extractor is.
const (
	krewScript    = "hack/krew-manifest.sh"
	brewScript    = "hack/homebrew-formula.sh"
	digestsScript = "hack/manifest-digests.sh"
)

// The names krew and kubectl between them fix. kubectl finds a plugin by file
// name, krew requires the manifest's file name, its `metadata.name` and the part
// of the binary after `kubectl-` to be one string — so none of these three is a
// choice this repository made, and a test that let them drift would be permitting
// a rename nothing else could catch.
const (
	krewPluginName  = "kuberecord"
	krewPluginBin   = "kubectl-" + krewPluginName
	krewAPIVersion  = "krew.googlecontainertools.github.com/v1alpha2"
	brewPlatformSet = 4 // darwin and linux, arm64 and amd64
)

// runHack invokes one of the hack/ scripts and returns stdout, stderr and the
// exit code. A signal or a failure to start is fatal — that is a broken test
// environment, not a result to interpret.
func runHack(t *testing.T, script string, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()

	cmd := exec.Command(repoPath(script), args...) // #nosec G204 -- test-controlled arguments
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
		t.Fatalf("run %s %v: %v", script, args, err)
	}
	return out.String(), errOut.String(), exitCode
}

// githubRepo reads <owner>/<name> out of go.mod, the way the Makefile's
// GITHUB_REPO does. Reading it rather than writing it down is what makes the URI
// assertions below say "this repository" instead of "the repository whose name I
// typed" — a fork that renamed itself must not keep publishing URLs here.
func githubRepo(t *testing.T) string {
	t.Helper()

	for line := range strings.SplitSeq(readFile(t, "go.mod"), "\n") {
		if after, found := strings.CutPrefix(strings.TrimSpace(line), "module github.com/"); found {
			return after
		}
	}
	t.Fatal("go.mod declares no github.com/ module path, so nothing decides where the URIs point")
	return ""
}

// fixtureVersion is the tag the stand-in archives below are named for. It is
// deliberately not a version this project has released or will: a test that
// happened to agree with the committed VERSION would keep passing after the
// generators started ignoring the version they were handed.
const fixtureVersion = "v9.9.9"

// distFixture writes one stand-in archive per platform into a temp directory and
// returns the directory, the <os/arch>=<archive> pairs the generators take, and
// the sha256 each archive really hashes to.
//
// The bytes are distinct per platform on purpose. Identical fixtures would hash
// identically, and "every platform got a digest" would pass for a generator that
// hashed one archive five times.
//
// The file names look like the real ones for readability only. Nothing here
// depends on the naming convention: the generators are handed the pairing, which
// is exactly the property that keeps a second copy of that convention out of
// them, and out of this file.
func distFixture(t *testing.T, platforms []string) (dir string, pairs []string,
	digests map[string]string) {
	t.Helper()

	dir = t.TempDir()
	digests = make(map[string]string, len(platforms))
	pairs = make([]string, 0, len(platforms))

	for _, platform := range platforms {
		goos, arch, ok := strings.Cut(platform, "/")
		if !ok {
			t.Fatalf("CLI_PLATFORMS carries %q, which is not <os>/<arch>", platform)
		}
		extension := ".tar.gz"
		if goos == "windows" {
			extension = ".zip"
		}
		name := fmt.Sprintf("kuberecord_%s_%s_%s%s", fixtureVersion, goos, arch, extension)
		body := []byte("stand-in archive for " + platform)
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
			t.Fatalf("write fixture %s: %v", name, err)
		}
		sum := sha256.Sum256(body)
		digests[platform] = hex.EncodeToString(sum[:])
		pairs = append(pairs, platform+"="+name)
	}
	return dir, pairs, digests
}

// krewPlugin is the part of the generated manifest these tests reason about.
// Parsed rather than grepped: "the file contains this digest" and "the entry for
// darwin/arm64 carries this digest" are different claims, and only the second one
// is worth making.
type krewPlugin struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Version          string `json:"version"`
		Homepage         string `json:"homepage"`
		ShortDescription string `json:"shortDescription"`
		Description      string `json:"description"`
		Caveats          string `json:"caveats"`
		Platforms        []struct {
			Selector struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"selector"`
			URI    string `json:"uri"`
			SHA256 string `json:"sha256"`
			Bin    string `json:"bin"`
			Files  []struct {
				From string `json:"from"`
				To   string `json:"to"`
			} `json:"files"`
		} `json:"platforms"`
	} `json:"spec"`
}

// TestKrewManifestIsGeneratedFromTheArchives is the acceptance criterion's first
// half, and the reason the manifest is generated at all.
//
// krew refuses an archive whose bytes do not hash to what the manifest says, so
// the digests are the whole of its integrity story. A hand-maintained manifest is
// wrong exactly once — and it then stays wrong until somebody's install fails on
// a platform nobody here runs, which is four of the five.
func TestKrewManifestIsGeneratedFromTheArchives(t *testing.T) {
	const version = fixtureVersion
	repo := githubRepo(t)
	platforms := cliPlatforms(t)
	dir, pairs, digests := distFixture(t, platforms)

	stdout, stderr, code := runHack(t, krewScript, append([]string{version, repo, dir}, pairs...)...)
	if code != 0 {
		t.Fatalf("%s exited %d: %s", krewScript, code, stderr)
	}

	var plugin krewPlugin
	if err := yaml.Unmarshal([]byte(stdout), &plugin); err != nil {
		t.Fatalf("the generated manifest is not YAML krew could read: %v\n%s", err, stdout)
	}

	if plugin.APIVersion != krewAPIVersion {
		t.Errorf("apiVersion is %q, want %q", plugin.APIVersion, krewAPIVersion)
	}
	if plugin.Kind != "Plugin" {
		t.Errorf("kind is %q, want Plugin", plugin.Kind)
	}
	if plugin.Metadata.Name != krewPluginName {
		t.Errorf("metadata.name is %q, want %q. krew requires the manifest's file name, this "+
			"field and the part of the binary name after `kubectl-` to be one string",
			plugin.Metadata.Name, krewPluginName)
	}
	if plugin.Spec.Version != version {
		t.Errorf("spec.version is %q, want %q", plugin.Spec.Version, version)
	}
	if want := "https://github.com/" + repo; plugin.Spec.Homepage != want {
		t.Errorf("spec.homepage is %q, want %q", plugin.Spec.Homepage, want)
	}

	if len(plugin.Spec.Platforms) != len(platforms) {
		t.Fatalf("the manifest declares %d platforms, but the release builds %d (%v). krew "+
			"installs nothing on a platform the manifest omits, and says so to the user "+
			"rather than to us", len(plugin.Spec.Platforms), len(platforms), platforms)
	}

	for i, platform := range platforms {
		t.Run(platform, func(t *testing.T) {
			goos, arch, _ := strings.Cut(platform, "/")
			entry := plugin.Spec.Platforms[i]

			if got := entry.Selector.MatchLabels["os"]; got != goos {
				t.Errorf("selector os is %q, want %q", got, goos)
			}
			if got := entry.Selector.MatchLabels["arch"]; got != arch {
				t.Errorf("selector arch is %q, want %q", got, arch)
			}

			// The digest is the archive's own, not any other platform's. The
			// fixtures differ per platform precisely so this can fail.
			if entry.SHA256 != digests[platform] {
				t.Errorf("sha256 is %s, but the %s archive hashes to %s",
					entry.SHA256, platform, digests[platform])
			}

			_, archive, _ := strings.Cut(pairs[i], "=")
			wantURI := fmt.Sprintf("https://github.com/%s/releases/download/%s/%s", repo, version, archive)
			if entry.URI != wantURI {
				t.Errorf("uri is\n  %s\nwant\n  %s", entry.URI, wantURI)
			}

			// Windows binaries carry the extension, and krew installs the file it
			// is told to: an extensionless `bin` installs a plugin Windows cannot
			// execute.
			wantBin := krewPluginBin
			if goos == "windows" {
				wantBin += ".exe"
			}
			if entry.Bin != wantBin {
				t.Errorf("bin is %q, want %q", entry.Bin, wantBin)
			}

			// `files` is narrowed on purpose. The archive also carries the
			// standalone name, which is the same bytes; letting krew copy
			// everything would put a second sixty-megabyte copy of one binary in
			// every user's krew store, under a name krew cannot invoke.
			var from []string
			for _, f := range entry.Files {
				from = append(from, f.From)
			}
			if want := []string{wantBin, "LICENSE"}; !slices.Equal(from, want) {
				t.Errorf("files copies %v, want %v", from, want)
			}
		})
	}
}

// TestKrewManifestSuitsTheIndex covers the conventions krew-index review applies,
// which are cheap to satisfy and expensive to find out about: the latency there is
// measured in weeks, so a manifest bounced for a trailing full stop costs a
// release cycle.
func TestKrewManifestSuitsTheIndex(t *testing.T) {
	const version = fixtureVersion
	platforms := cliPlatforms(t)
	dir, pairs, _ := distFixture(t, platforms)

	stdout, stderr, code := runHack(t, krewScript,
		append([]string{version, githubRepo(t), dir}, pairs...)...)
	if code != 0 {
		t.Fatalf("%s exited %d: %s", krewScript, code, stderr)
	}

	var plugin krewPlugin
	if err := yaml.Unmarshal([]byte(stdout), &plugin); err != nil {
		t.Fatalf("parse the generated manifest: %v", err)
	}

	// krew renders this in a column of `kubectl krew search`.
	const shortDescriptionLimit = 50
	short := plugin.Spec.ShortDescription
	if short == "" {
		t.Error("spec.shortDescription is empty; it is the line `kubectl krew search` prints")
	}
	if len(short) > shortDescriptionLimit {
		t.Errorf("spec.shortDescription is %d characters (%q); krew-index asks for at most %d",
			len(short), short, shortDescriptionLimit)
	}
	if strings.HasSuffix(short, ".") {
		t.Errorf("spec.shortDescription ends with a full stop (%q); krew-index asks that it "+
			"does not, because it is a label rather than a sentence", short)
	}
	if plugin.Spec.Description == "" {
		t.Error("spec.description is empty; it is the whole of what `kubectl krew info` shows")
	}

	// The caveat that matters, because it is the one thing about this plugin a
	// user is likely to get wrong: krew installs the plugin name only, and the
	// standalone binary — the one an auditor with an archive and no cluster
	// wants — is not on their PATH afterwards.
	if !strings.Contains(plugin.Spec.Caveats, "brew install") {
		t.Error("spec.caveats no longer points at the Homebrew tap. krew installs the plugin " +
			"name only, so a user who wanted the standalone `kuberecord` gets no hint of " +
			"where it is")
	}
}

// TestGeneratorsRefuseAnIncompleteRelease is the non-vacuity proof for both
// generators.
//
// A generator that quietly skipped what it could not find would emit a shorter
// document, and a shorter document is not a smaller release — it is a release
// that installs on some platforms and 404s on others, with nothing having failed.
func TestGeneratorsRefuseAnIncompleteRelease(t *testing.T) {
	const version = fixtureVersion
	repo := githubRepo(t)
	platforms := cliPlatforms(t)

	tests := []struct {
		name   string
		script string
		// remove is the platform whose archive is deleted before the run.
		remove string
		// names is what the refusal must mention, so the failure is diagnosable.
		names string
	}{
		{
			name:   "krew, with the linux/arm64 archive missing",
			script: krewScript,
			remove: "linux/arm64",
			names:  "linux/arm64",
		},
		{
			name:   "homebrew, with the darwin/arm64 archive missing",
			script: brewScript,
			remove: "darwin/arm64",
			names:  "darwin/arm64",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, pairs, _ := distFixture(t, platforms)

			kept := make([]string, 0, len(pairs))
			for _, pair := range pairs {
				platform, archive, _ := strings.Cut(pair, "=")
				if platform == tc.remove {
					if err := os.Remove(filepath.Join(dir, archive)); err != nil {
						t.Fatalf("remove the %s fixture: %v", tc.remove, err)
					}
				}
				kept = append(kept, pair)
			}

			_, stderr, code := runHack(t, tc.script, append([]string{version, repo, dir}, kept...)...)
			if code == 0 {
				t.Fatalf("%s succeeded with the %s archive missing; it would have published a "+
					"document naming a URL that 404s for exactly those users",
					tc.script, tc.remove)
			}
			if !strings.Contains(stderr, tc.names) {
				t.Errorf("%s refused, but without naming %s:\n%s", tc.script, tc.names, stderr)
			}
		})
	}
}

// TestHomebrewFormulaCoversEveryPlatformBrewRunsOn is the formula's half of the
// acceptance criterion.
//
// Homebrew picks the block matching the machine it is on, so a missing block is
// not a smaller formula: it is one that fails for a quarter of its users and works
// perfectly for whoever tested it.
func TestHomebrewFormulaCoversEveryPlatformBrewRunsOn(t *testing.T) {
	const version = fixtureVersion
	repo := githubRepo(t)
	dir, pairs, digests := distFixture(t, cliPlatforms(t))

	stdout, stderr, code := runHack(t, brewScript, append([]string{version, repo, dir}, pairs...)...)
	if code != 0 {
		t.Fatalf("%s exited %d: %s", brewScript, code, stderr)
	}

	// Windows has no brew, and the generator says so rather than narrowing the
	// release without comment.
	if !strings.Contains(stderr, "windows/amd64") {
		t.Errorf("the formula generator dropped the Windows archive silently. Its stderr was:\n%s", stderr)
	}
	if strings.Contains(stdout, "windows") {
		t.Error("the formula names a Windows archive; brew does not run there, and a URL " +
			"nothing can select is a URL nothing checks")
	}

	for want, why := range map[string]string{
		"class Kuberecord < Formula":          "brew derives the class name from the file name",
		`version "9.9.9"`:                     "a Homebrew version is plain semver; the `v` is a tag convention",
		`license "Apache-2.0"`:                "the licence the archives carry",
		`bin.install "kuberecord"`:            "the standalone name, which is what brew is the channel for",
		`bin.install "` + krewPluginBin + `"`: "kubectl finds a plugin by file name, so both names ship",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the formula does not contain %q: %s", want, why)
		}
	}

	// The one check that catches a formula pointing at the wrong tag's archive:
	// the version is stamped into the binary, so a stale URL reports a version
	// that is not this formula's.
	if !strings.Contains(stdout, `shell_output("#{bin}/kuberecord version")`) {
		t.Error("the formula's `test do` block no longer runs `kuberecord version`, so a URL " +
			"left pointing at the previous release would install cleanly and be wrong")
	}

	// Every brew platform, with its own archive's digest.
	pairsOut, stderr, code := runHack(t, digestsScript, writeTemp(t, "formula.rb", stdout))
	if code != 0 {
		t.Fatalf("%s could not read the formula (%d): %s", digestsScript, code, stderr)
	}
	got := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(pairsOut), "\n") {
		url, digest, _ := strings.Cut(line, " ")
		got[url] = digest
	}
	if len(got) != brewPlatformSet {
		t.Fatalf("the formula names %d downloads, want %d (darwin and linux, arm64 and amd64)",
			len(got), brewPlatformSet)
	}
	for _, platform := range []string{"darwin/arm64", "darwin/amd64", "linux/arm64", "linux/amd64"} {
		archive := archiveFor(t, pairs, platform)
		url := fmt.Sprintf("https://github.com/%s/releases/download/%s/%s", repo, version, archive)
		if got[url] != digests[platform] {
			t.Errorf("%s: the formula serves %s with digest %q, want %q",
				platform, url, got[url], digests[platform])
		}
	}
}

// archiveFor returns the archive one <os/arch>=<archive> pair names.
func archiveFor(t *testing.T, pairs []string, platform string) string {
	t.Helper()
	for _, pair := range pairs {
		if p, archive, _ := strings.Cut(pair, "="); p == platform {
			return archive
		}
	}
	t.Fatalf("no pair for %s", platform)
	return ""
}

// writeTemp puts content in a temp file with the given name and returns its path.
// The name matters: hack/manifest-digests.sh picks its shape from the extension.
func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// TestManifestDigestsRefusesAnUnpairedDownload is the extractor's non-vacuity
// proof, and it guards the verification rather than the generation.
//
// A url with no sha256 beside it must be an error. Dropped, it would be a download
// the release never checks — and the verify target would report a smaller number
// and call it a pass.
func TestManifestDigestsRefusesAnUnpairedDownload(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		content string
	}{
		{
			name: "a krew platform with no digest",
			file: "kuberecord.yaml",
			content: "  platforms:\n" +
				"  - uri: https://example.invalid/a.tar.gz\n" +
				"    bin: kubectl-kuberecord\n" +
				"  - uri: https://example.invalid/b.tar.gz\n" +
				"    sha256: " + strings.Repeat("a", 64) + "\n",
		},
		{
			name: "a formula url with no digest",
			file: "kuberecord.rb",
			content: "  on_macos do\n" +
				`      url "https://example.invalid/a.tar.gz"` + "\n" +
				`      url "https://example.invalid/b.tar.gz"` + "\n" +
				`      sha256 "` + strings.Repeat("a", 64) + `"` + "\n",
		},
		{
			name:    "a document naming nothing at all",
			file:    "kuberecord.yaml",
			content: "apiVersion: krew.googlecontainertools.github.com/v1alpha2\nkind: Plugin\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, stderr, code := runHack(t, digestsScript, writeTemp(t, tc.file, tc.content))
			if code == 0 {
				t.Fatalf("%s accepted %s. A download it silently drops is one the release "+
					"never checks, and the verify target would count fewer and pass",
					digestsScript, tc.name)
			}
			if stderr == "" {
				t.Error("it refused without saying why")
			}
		})
	}
}

// TestDistributionDocumentsAreReleaseAssets is the wiring: generated in the right
// place, checksummed by name, and attached.
//
// Being lines in checksums.txt is the whole of why they need no evidence of their
// own — the artifact attestation takes its subjects from that file and the one
// cosign signature is over it, so both documents inherit both.
func TestDistributionDocumentsAreReleaseAssets(t *testing.T) {
	makefile := readFile(t, "Makefile")

	// Required by name, on the same argument install.yaml and the CLI archives
	// are: a checksums file that silently omits an asset is worse than none.
	const required = `assets="$$assets $(notdir $(RELEASE_KREW_MANIFEST)) $(notdir $(RELEASE_BREW_FORMULA))"`
	if !strings.Contains(makefile, required) {
		t.Error("release-checksums no longer requires the krew manifest and the Homebrew " +
			"formula by name, so a release that failed to generate one would publish a " +
			"complete-looking checksums file with no way to install the CLI in it")
	}

	// And they have to exist before the first checksum run, for the same reason
	// the archives do.
	start := strings.Index(makefile, "release-artifacts: release-verify-version")
	if start < 0 {
		t.Fatal("the Makefile no longer defines release-artifacts")
	}
	recipe := makefile[start:]
	order := []string{
		"$(MAKE) release-cli",
		"$(MAKE) release-krew-manifest",
		"$(MAKE) release-brew-formula",
		"$(MAKE) release-checksums",
		"$(MAKE) release-krew-verify",
	}
	previous := -1
	for _, step := range order {
		at := strings.Index(recipe, step)
		if at < 0 {
			t.Fatalf("release-artifacts no longer runs `%s`", step)
		}
		if at < previous {
			t.Errorf("release-artifacts runs `%s` out of order; the sequence must be %v",
				step, order)
		}
		previous = at
	}

	// Stale copies from an earlier version in the same directory would be
	// checksummed and published, like every other per-tag output.
	if !strings.Contains(makefile, `"$(RELEASE_KREW_MANIFEST)" "$(RELEASE_BREW_FORMULA)" \`) {
		t.Error("release-artifacts no longer removes the previous run's krew manifest and " +
			"formula, so a failed generation would leave the last version's in place")
	}

	// Attached to the Release, and to a rehearsal's workflow run — on a rehearsal
	// that is the only place they exist, and diffing a candidate's manifest
	// against the last release's is most of the value of rehearsing.
	workflow := readFile(t, releaseWorkflowPath)
	for _, asset := range []string{"dist/release/kuberecord.yaml", "dist/release/kuberecord.rb"} {
		if strings.Count(workflow, asset) < 2 {
			t.Errorf("%s is not both uploaded to the workflow run and attached to the Release",
				asset)
		}
	}
}

// TestDistributionDigestsAreVerifiedInCI is the acceptance criterion's "a CI check
// asserts the manifest's digests match the published archives", and it is two
// checks because they are two different claims.
//
// The local one says the documents describe what was built; the published one says
// the URLs in them resolve to it. Only the second catches an asset that never
// uploaded or a Release page that spells a file name differently, and only the
// first can run before there is anything published.
func TestDistributionDigestsAreVerifiedInCI(t *testing.T) {
	wf := parseReleaseWorkflow(t)

	var local, published int
	for jobName, job := range wf.Jobs {
		for _, step := range job.Steps {
			switch {
			case strings.Contains(step.Run, "make release-krew-verify-published"):
				published++
				if step.If != "env.DRY_RUN == 'false'" {
					t.Errorf("%s/%q fetches published assets under `if: %s`; a rehearsal has "+
						"nothing published to fetch", jobName, step.Name, step.If)
				}
			case strings.Contains(step.Run, "make release-krew-verify"):
				local++
				if step.If != "" {
					t.Errorf("%s/%q re-derives the digests under `if: %s`. Hashing a local file "+
						"publishes nothing, and a manifest that stopped describing the "+
						"archives is what a rehearsal exists to find", jobName, step.Name, step.If)
				}
			}
		}
	}
	if local == 0 {
		t.Error("no step runs `make release-krew-verify`, so nothing checks that the two " +
			"documents describe the archives that were built")
	}
	if published == 0 {
		t.Error("no step runs `make release-krew-verify-published`, so nothing checks that " +
			"the URLs krew and brew hand out actually serve those archives")
	}

	// The published check has to come after the Release exists — the assets are
	// not downloadable until then.
	workflow := readFile(t, releaseWorkflowPath)
	create := strings.Index(workflow, "gh release create")
	verify := strings.Index(workflow, "make release-krew-verify-published")
	if create < 0 || verify < 0 {
		t.Fatal("release.yml no longer both creates the Release and verifies the published URLs")
	}
	if verify < create {
		t.Error("release.yml verifies the published URLs before it creates the Release, so " +
			"every one of them would 404")
	}

	// And the cheap half runs on every pull request, not only at tag time. The
	// drift it catches — an archive renamed in the Makefile, URIs generated from
	// the old name — passes every other check in the repository.
	if !workflowRuns(t, ".github/workflows/test.yml", "make release-krew-verify") {
		t.Error("no step in .github/workflows/test.yml runs `make release-krew-verify`, so " +
			"the manifest is only ever checked at tag time — which is after the release " +
			"it would have stopped")
	}
}

// TestHomebrewTapIsUpdatedByTheRelease pins the shape of the one job in this
// workflow that writes to another repository.
func TestHomebrewTapIsUpdatedByTheRelease(t *testing.T) {
	wf := parseReleaseWorkflow(t)

	tap, ok := wf.Jobs["tap"]
	if !ok {
		t.Fatal("release.yml has no tap job, so `brew install kuberecord/tap/kuberecord` " +
			"serves whatever version somebody last pushed by hand")
	}

	// It must not start before the Release exists: the formula it pushes is
	// downloaded off the Release page.
	needs := fmt.Sprint(tap.Needs)
	if !strings.Contains(needs, "publish") {
		t.Errorf("the tap job needs %v, which does not include publish — so it could run "+
			"before the Release it downloads the formula from exists", tap.Needs)
	}

	// The token is the whole reason this is a job of its own, and it is the only
	// place in the workflow that a secret other than GITHUB_TOKEN appears.
	workflow := readFile(t, releaseWorkflowPath)
	if !strings.Contains(workflow, "secrets.HOMEBREW_TAP_TOKEN") {
		t.Error("nothing supplies HOMEBREW_TAP_TOKEN. A workflow's own GITHUB_TOKEN is scoped " +
			"to this repository and cannot write to the tap")
	}

	var fetches, pushes bool
	for _, step := range tap.Steps {
		if strings.Contains(step.Run, "make release-brew-fetch") {
			fetches = true
		}
		if strings.Contains(step.Run, "make release-brew-push") {
			pushes = true
		}
	}
	if !fetches {
		t.Error("the tap job does not run `make release-brew-fetch`, so the formula it pushes " +
			"is one it built rather than the one the release signed")
	}
	if !pushes {
		t.Error("the tap job does not run `make release-brew-push`")
	}
}

// TestTapRefusesWhatItMustRefuse runs the target rather than reading it. Both
// guards fire before anything is cloned, so this needs no token and no network.
//
// It does need a formula, and it supplies one rather than using whatever is in
// dist/release/. The token guard is the *last* of three, so a run that reached it
// only because an earlier `make release-artifacts` had left a file behind is a
// test that passes on the machine that generated one and fails on a clean
// checkout — which is exactly what it did. RELEASE_BREW_FORMULA is `?=` in the
// Makefile, so pointing it at a temporary file is enough to make the precondition
// the test's own.
func TestTapRefusesWhatItMustRefuse(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantOK  bool
		says    string
	}{
		{
			// `brew install` cannot ask for a stable version, so a tap carrying a
			// candidate hands it to everybody. Exit 0, because the release
			// happened and this is not a failure of it — but loudly.
			name:    "a prerelease is not pushed",
			version: "v9.9.9-rc.1",
			wantOK:  true,
			says:    "prerelease",
		},
		{
			// Absent, the push would silently do nothing, and the tap would stop
			// updating with nothing red anywhere.
			name:    "a missing token is a failure, not a skip",
			version: "v9.9.9",
			wantOK:  false,
			says:    "HOMEBREW_TAP_TOKEN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			formula := writeTemp(t, "kuberecord.rb", "# a stand-in; no guard below reads it\n")

			cmd := exec.Command("make", "release-brew-push",
				"RELEASE_VERSION="+tc.version, "RELEASE_BREW_FORMULA="+formula)
			cmd.Dir = repoPath()
			// An inherited token would turn the second case into a real clone.
			cmd.Env = append(os.Environ(), "HOMEBREW_TAP_TOKEN=")

			out, err := cmd.CombinedOutput()
			if gotOK := err == nil; gotOK != tc.wantOK {
				t.Fatalf("`make release-brew-push RELEASE_VERSION=%s` succeeded=%v, want %v:\n%s",
					tc.version, gotOK, tc.wantOK, out)
			}
			if !strings.Contains(string(out), tc.says) {
				t.Errorf("it did not mention %q:\n%s", tc.says, out)
			}
		})
	}
}

// TestKrewIndexSubmissionIsAMaintainersCommand is the boundary this task drew on
// purpose.
//
// Submitting to krew-index is a pull request against a repository this project
// does not own, and a tag push must not open one. It also could not work: krew-index
// CI fetches every URI in the manifest, so a PR raised before the assets exist
// fails on arrival and spends weeks of review latency getting nowhere.
func TestKrewIndexSubmissionIsAMaintainersCommand(t *testing.T) {
	makefile := readFile(t, "Makefile")
	if !strings.Contains(makefile, "krew-index-pr: ##") {
		t.Fatal("the Makefile no longer defines krew-index-pr, so submitting to krew-index is " +
			"a sequence of commands somebody retypes each release")
	}
	if !strings.Contains(makefile, "KREW_INDEX_REPO ?= kubernetes-sigs/krew-index") {
		t.Error("krew-index-pr no longer names kubernetes-sigs/krew-index")
	}

	// Never called by a workflow. Not any workflow — the point is that no
	// automated trigger in this repository opens a pull request on another one.
	//
	// It looks for an *invocation* rather than the string, because release.yml's
	// step summary tells a maintainer to run this and would otherwise be indicted
	// by the test that exists to keep it a maintainer's job. A line that starts
	// with `make` runs it; a line that mentions it does not.
	dir := repoPath(".github", "workflows")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.ToSlash(filepath.Join(".github", "workflows", entry.Name()))
		for _, job := range parseWorkflow(t, path).Jobs {
			for _, step := range job.Steps {
				for line := range strings.SplitSeq(step.Run, "\n") {
					line = strings.TrimSpace(line)
					if !strings.HasPrefix(line, "make ") && !strings.HasPrefix(line, "$(MAKE) ") {
						continue
					}
					if strings.Contains(line, "krew-index-pr") {
						t.Errorf("%s runs `%s`. A tag push must not open a pull request against "+
							"somebody else's repository, and it would fail anyway: krew-index "+
							"fetches every URI, and the assets do not exist until the release "+
							"is published", path, line)
					}
				}
			}
		}
	}

	// And the procedure is written down, because it is the one release step a
	// maintainer has to remember.
	releasing := readFile(t, "docs/RELEASING.md")
	for _, want := range []string{"make krew-index-pr", "kubernetes-sigs/krew-index", "HOMEBREW_TAP_TOKEN"} {
		if !strings.Contains(releasing, want) {
			t.Errorf("docs/RELEASING.md does not mention %q", want)
		}
	}
}

// TestInstallPathsAreDocumentedInOrder is the acceptance criterion about the
// documentation, and it checks the order rather than the wording.
//
// The order is the recommendation. krew first because it is how a kubectl user
// finds a plugin and the one that needs no decision; `go install` last because it
// gets you one of the two names, no release stamp and nothing signed. A page that
// listed them the other way round would be recommending the worst channel.
func TestInstallPathsAreDocumentedInOrder(t *testing.T) {
	paths := []struct{ name, marker string }{
		{"krew", "kubectl krew install kuberecord"},
		{"Homebrew", "brew install kuberecord/tap/kuberecord"},
		{"the release archive", "releases/download/"},
		{"go install", "go install github.com/kuberecord/kuberecord/cmd/kubectl-kuberecord"},
	}

	pages := []struct{ page, heading string }{
		{"README.md", "## Installing the CLI"},
		{"docs/CLI.md", "## Installing"},
	}

	for _, p := range pages {
		t.Run(p.page, func(t *testing.T) {
			// Scoped to the section, not the whole page: both documents name a
			// release download elsewhere, for the operator's install manifest.
			body := readFile(t, p.page)
			start := strings.Index(body, p.heading+"\n")
			if start < 0 {
				t.Fatalf("%s has no %q section, so it does not tell a reader how to install "+
					"the CLI at all", p.page, p.heading)
			}
			section := body[start+len(p.heading):]
			if end := strings.Index(section, "\n## "); end > 0 {
				section = section[:end]
			}

			previous := -1
			for _, path := range paths {
				at := strings.Index(section, path.marker)
				if at < 0 {
					t.Errorf("%s does not show %s (%q)", p.page, path.name, path.marker)
					continue
				}
				if at < previous {
					t.Errorf("%s shows %s out of order; the four channels are listed krew, "+
						"Homebrew, direct download, go install", p.page, path.name)
				}
				previous = at
			}
		})
	}
}
