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

// Package docs holds the checks that keep kuberecord's user-facing instructions
// honest (Task 3.4).
//
// Documentation is the one artifact in this repository that nothing compiles: a
// link that rots when a page moves, a quickstart that names a manifest somebody
// deleted, two files that are supposed to share a password and quietly stop, a
// removed environment variable that creeps back into a README because it is
// still in someone's shell history. Each of those ships silently and is
// discovered by the person who trusted it — which, for the quickstart, is by
// definition someone evaluating the project for the first time.
//
// These tests run under `make test`, need neither a cluster nor a database, and
// read the repository as a user would.
package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
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

//
// The env-var era is over (D5)
//

// bannedConfig is the configuration surface Phase 1 removed outright. There is no
// compatibility shim, so a document still instructing a reader to set one of
// these is instructing them to do something that cannot work — a worse failure
// than a stale sentence, because the reader has no way to tell it is stale.
//
// The identifier patterns use \b, which treats `_` as a word character. That is
// what makes `E2E_CH_PASSWORD`, `CH_IT_PASSWORD`, `CH_TEST_ADDR` and
// `CLICKHOUSE_PASSWORD` — all current, all legitimate — not matches, while a bare
// `CH_PASSWORD` is one.
var bannedConfig = []struct {
	name    string
	pattern *regexp.Regexp
	became  string
}{
	{"WATCHED_GVKS", regexp.MustCompile(`\bWATCHED_GVKS\b`), "StreamRule / ClusterStreamRule spec.resources"},
	{"CH_ADDR", regexp.MustCompile(`\bCH_ADDR\b`), "ClickHouseSink spec.connection.addr"},
	{"CH_DATABASE", regexp.MustCompile(`\bCH_DATABASE\b`), "ClickHouseSink spec.connection.database"},
	{"CH_USERNAME", regexp.MustCompile(`\bCH_USERNAME\b`), "ClickHouseSink spec.connection.username"},
	{"CH_PASSWORD", regexp.MustCompile(`\bCH_PASSWORD\b`), "the Secret named by spec.connection.credentialsSecretRef"},
	{"CH_TABLE", regexp.MustCompile(`\bCH_TABLE\b`), "the frozen schema v1 table names"},
	{"--ch-addr", regexp.MustCompile(`--ch-addr\b`), "ClickHouseSink spec.connection.addr"},
	{"--ch-database", regexp.MustCompile(`--ch-database\b`), "ClickHouseSink spec.connection.database"},
	{"--ch-username", regexp.MustCompile(`--ch-username\b`), "ClickHouseSink spec.connection.username"},
	{"--ch-password", regexp.MustCompile(`--ch-password\b`), "the Secret named by spec.connection.credentialsSecretRef"},
}

// allowedToNameBannedConfig are the files whose job is to say what these
// settings *became*. A migration table that could not name the thing being
// migrated from would be useless, so they are exempt — and the exemption is a
// short, explicit list rather than a pattern, so adding to it is a decision
// somebody makes on purpose.
var allowedToNameBannedConfig = map[string]string{
	"CHANGELOG.md":           "the removal record and its migration table",
	"kuberecord-backlog.md":  "the roadmap that specified the removal",
	"CLAUDE.md":              "the contributor guide, where D5 records what was removed",
	"task.txt":               "the task brief handed to the agent",
	"test/docs/docs_test.go": "this test",
}

// skippedDirs are directories with nothing user-facing in them: build output,
// tool binaries and version control internals.
var skippedDirs = map[string]bool{
	".git": true, "bin": true, "node_modules": true, "testbin": true,
}

// skippedPaths are directories skipped by repository-relative path rather than by
// name, because their name is not distinctive enough to skip everywhere.
//
// dist/release/ is what `make release-artifacts` fills for one tag. Its
// RELEASE_NOTES.md is a verbatim copy of a CHANGELOG.md section, and CHANGELOG.md
// is exempt from the check below by name — so scanning the copy would fail the
// build for the migration table the original is required to keep. The rest of
// dist/ is *not* skipped: dist/install.yaml is committed and user-facing.
var skippedPaths = map[string]bool{
	"dist/release": true,
}

// TestNoEnvVarEraConfiguration is the grep check Task 3.4 asks for, as a test
// rather than as a command somebody has to remember to run.
func TestNoEnvVarEraConfiguration(t *testing.T) {
	root := repoPath()

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)

		if d.IsDir() {
			if skippedDirs[d.Name()] || skippedPaths[rel] {
				return filepath.SkipDir
			}
			return nil
		}
		if _, exempt := allowedToNameBannedConfig[rel]; exempt {
			return nil
		}
		// Coverage profiles and other large build artifacts are not instructions,
		// and reading them costs more than the check is worth.
		if rel == "cover.out" {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			return infoErr
		}
		if info.Size() > 4<<20 {
			return nil
		}

		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		content := string(raw)
		for _, banned := range bannedConfig {
			if loc := banned.pattern.FindStringIndex(content); loc != nil {
				line := 1 + strings.Count(content[:loc[0]], "\n")
				t.Errorf("%s:%d names %s, which Phase 1 removed with no compatibility shim (D5). It is now %s.",
					rel, line, banned.name, banned.became)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
}

// TestMigrationRecordStillNamesThem is the other half of the check above, and
// the reason it is worth writing down: "the old names appear nowhere" would be
// satisfied by deleting the migration table, which would leave an upgrading user
// with no way to find out what their configuration became.
func TestMigrationRecordStillNamesThem(t *testing.T) {
	changelog := readFile(t, "CHANGELOG.md")
	for _, name := range []string{"WATCHED_GVKS", "CH_ADDR", "CH_DATABASE", "CH_USERNAME", "CH_PASSWORD"} {
		if !strings.Contains(changelog, name) {
			t.Errorf("CHANGELOG.md no longer names %s; the migration table is how a user finds out what it became", name)
		}
	}
}

//
// The quickstart is self-consistent
//

// quickstartFiles is every file the quickstart promises. The acceptance criteria
// name a kind config, a ClickHouse manifest, Secret/Sink/Rule samples and a make
// target; this is that list, plus the pieces that make it runnable.
var quickstartFiles = []string{
	"examples/quickstart/README.md",
	"examples/quickstart/kind.yaml",
	"examples/quickstart/clickhouse.yaml",
	"examples/quickstart/secret.yaml",
	"examples/quickstart/sink.yaml",
	"examples/quickstart/rule.yaml",
	"examples/quickstart/demo.yaml",
	"examples/quickstart/operator/kustomization.yaml",
	"examples/quickstart/quickstart.sh",
}

func TestQuickstartFilesExist(t *testing.T) {
	for _, rel := range quickstartFiles {
		t.Run(rel, func(t *testing.T) {
			info, err := os.Stat(repoPath(rel))
			if err != nil {
				t.Fatalf("%s is promised by the quickstart but missing: %v", rel, err)
			}
			if info.Size() == 0 {
				t.Fatalf("%s is empty", rel)
			}
		})
	}

	script := repoPath("examples/quickstart/quickstart.sh")
	info, err := os.Stat(script)
	if err != nil {
		t.Fatalf("stat quickstart.sh: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Errorf("examples/quickstart/quickstart.sh is not executable; `make quickstart` runs it directly")
	}
}

// TestQuickstartPasswordsAgree guards the failure that would be most confusing to
// hit as a newcomer: the demo password is written down in three places — the
// server's own Secret, the operator's credentials Secret, and the script that
// queries with it — and a quickstart that ends in an authentication error is
// worse than no quickstart at all.
//
// Three places rather than one because each is a different system's input, and
// collapsing them would mean templating a manifest the README tells you to read.
func TestQuickstartPasswordsAgree(t *testing.T) {
	const want = "quickstart"

	tests := []struct {
		file    string
		pattern *regexp.Regexp
	}{
		{"examples/quickstart/clickhouse.yaml", regexp.MustCompile(`(?m)^\s*password:\s*"([^"]*)"`)},
		{"examples/quickstart/secret.yaml", regexp.MustCompile(`(?m)^\s*password:\s*"([^"]*)"`)},
		{"examples/quickstart/quickstart.sh", regexp.MustCompile(`(?m)^QS_CH_PASSWORD="([^"]*)"`)},
	}
	for _, tc := range tests {
		t.Run(tc.file, func(t *testing.T) {
			m := tc.pattern.FindStringSubmatch(readFile(t, tc.file))
			if m == nil {
				t.Fatalf("no password found in %s; the check that the three agree cannot run", tc.file)
			}
			if m[1] != want {
				t.Errorf("%s carries password %q, but the other quickstart files use %q", tc.file, m[1], want)
			}
		})
	}
}

// TestQuickstartImageTagAgrees covers the same class of mistake one layer down:
// kustomize cannot read an environment variable, so the image the script builds
// and side-loads is named independently in the overlay. If the two drift, the
// manager pod sits in ImagePullBackOff — which reads as a broken cluster rather
// than as a typo.
func TestQuickstartImageTagAgrees(t *testing.T) {
	script := readFile(t, "examples/quickstart/quickstart.sh")
	overlay := readFile(t, "examples/quickstart/operator/kustomization.yaml")

	m := regexp.MustCompile(`(?m)^IMG="([^"]*)"`).FindStringSubmatch(script)
	if m == nil {
		t.Fatal("quickstart.sh declares no IMG")
	}
	name, tag, ok := strings.Cut(m[1], ":")
	if !ok {
		t.Fatalf("IMG=%q in quickstart.sh has no tag", m[1])
	}
	if !strings.Contains(overlay, "newName: "+name) {
		t.Errorf("the overlay does not set newName: %s, but quickstart.sh builds %s", name, m[1])
	}
	if !strings.Contains(overlay, "newTag: "+tag) {
		t.Errorf("the overlay does not set newTag: %s, but quickstart.sh builds %s", tag, m[1])
	}
}

// TestQuickstartIsWiredUp checks the seams between the script and the two things
// that invoke it. A make target or a CI job pointing at a path that moved fails
// only when somebody runs it, which for the CI job means "on the commit after
// the one that broke it".
func TestQuickstartIsWiredUp(t *testing.T) {
	makefile := readFile(t, "Makefile")
	for _, want := range []string{"quickstart:", "quickstart-down:", "examples/quickstart/quickstart.sh"} {
		if !strings.Contains(makefile, want) {
			t.Errorf("the Makefile no longer contains %q", want)
		}
	}

	workflow := readFile(t, ".github/workflows/quickstart.yml")
	if !strings.Contains(workflow, "make quickstart") {
		t.Error(".github/workflows/quickstart.yml no longer runs `make quickstart`")
	}
	// The budget is what makes the ten-minute claim tested rather than asserted.
	// Without it the job still proves rows arrive, but says nothing about when.
	if !strings.Contains(workflow, "QUICKSTART_BUDGET_SECONDS") {
		t.Error(".github/workflows/quickstart.yml sets no QUICKSTART_BUDGET_SECONDS; " +
			"the ten-minute claim would go untested")
	}
}

//
// The README says what it must, and every link resolves
//

// TestREADMEStructure asserts the sections Task 3.4 requires. It checks for
// presence, not for wording: prose should be free to improve, but a README that
// has quietly lost its quickstart or its positioning has lost the thing that
// makes a stranger try the project.
func TestREADMEStructure(t *testing.T) {
	readme := readFile(t, "README.md")

	tests := []struct {
		name string
		want string
		why  string
	}{
		{"positioning", "Git blame for your Kubernetes cluster", "the one-line answer to \"what is this?\""},
		{"why section", "\n## Why\n", "the problem statement"},
		{"audit-log complementarity", "audit log", "readers arrive believing this duplicates their audit log"},
		{"quickstart", "\n## Quickstart\n", "the evaluation path"},
		{"make quickstart", "make quickstart", "the one command the quickstart is"},
		{"first five queries", "\n## Your first five queries\n", "the read path, which is the point of writing the rows"},
		{"architecture", "\n## Architecture\n", "the two-plane, CRD-driven design"},
		{"control plane", "CONTROL PLANE", "the diagram must show both tiers"},
		{"data plane", "DATA PLANE", "the diagram must show both tiers"},
		{"documentation index", "\n## Documentation\n", "the map to the reference docs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(readme, tc.want) {
				t.Errorf("README.md no longer contains %q — %s", tc.want, tc.why)
			}
		})
	}

	// The four reference documents the acceptance criteria name explicitly.
	for _, doc := range []string{"docs/SCHEMA.md", "docs/RBAC.md", "docs/PERFORMANCE.md", "docs/QUERIES.md"} {
		t.Run("links "+doc, func(t *testing.T) {
			if !strings.Contains(readme, "("+doc+")") {
				t.Errorf("README.md no longer links %s", doc)
			}
		})
	}

	if !strings.Contains(readme, "examples/quickstart/") {
		t.Error("README.md no longer points at examples/quickstart/")
	}
}

// markdownLink matches inline links, `[text](target)`, and reference definitions,
// `[label]: target`.
var (
	inlineLink = regexp.MustCompile(`\[[^\]]*\]\(([^)\s]+)(?:\s+"[^"]*")?\)`)
	refLink    = regexp.MustCompile(`(?m)^\[[^\]]+\]:\s+(\S+)`)
)

// headingLine matches an ATX Markdown heading.
var headingLine = regexp.MustCompile(`(?m)^(#{1,6})\s+(.*?)\s*$`)

// nonSlugChar is everything GitHub drops when it derives a heading's anchor.
// Backticks, em dashes, slashes and punctuation all vanish; spaces become
// hyphens, and the result is lowercased.
var nonSlugChar = regexp.MustCompile(`[^a-z0-9 \-_]`)

func slugify(heading string) string {
	s := strings.ToLower(heading)
	s = nonSlugChar.ReplaceAllString(s, "")
	return strings.ReplaceAll(s, " ", "-")
}

// anchorsOf returns the set of fragment identifiers a Markdown document defines.
func anchorsOf(content string) map[string]bool {
	out := map[string]bool{}
	for _, m := range headingLine.FindAllStringSubmatch(content, -1) {
		out[slugify(m[2])] = true
	}
	return out
}

// publishedPages are every Markdown document a reader is expected to follow
// links through. docs/ is enumerated rather than listed so a new page is covered
// the moment it is added.
func publishedPages(t *testing.T) []string {
	t.Helper()
	pages := []string{
		"README.md",
		"CHANGELOG.md",
		"examples/quickstart/README.md",
		"deploy/charts/kuberecord/README.md",
	}
	entries, err := os.ReadDir(repoPath("docs"))
	if err != nil {
		t.Fatalf("read docs/: %v", err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			pages = append(pages, filepath.ToSlash(filepath.Join("docs", e.Name())))
		}
	}
	return pages
}

// TestMarkdownLinksResolve walks every published Markdown page and checks that
// each relative link points at a file that exists — and, where the link carries a
// fragment, at a heading that exists inside it.
//
// This is the check that earns its keep when documentation is reorganised: moving
// a section out of the README into docs/ is a good change that silently breaks
// every link into it, and a link to a heading that is merely *gone* still passes
// every check a build system does. Nothing else in the repository would notice.
func TestMarkdownLinksResolve(t *testing.T) {
	for _, page := range publishedPages(t) {
		t.Run(page, func(t *testing.T) {
			content := readFile(t, page)
			dir := filepath.Dir(repoPath(page))

			targets := make([]string, 0, 32)
			for _, m := range inlineLink.FindAllStringSubmatch(content, -1) {
				targets = append(targets, m[1])
			}
			for _, m := range refLink.FindAllStringSubmatch(content, -1) {
				targets = append(targets, m[1])
			}

			for _, target := range targets {
				// External links are somebody else's problem: this test is about
				// the repository's own structure.
				if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") ||
					strings.HasPrefix(target, "mailto:") {
					continue
				}
				path, fragment, hasFragment := strings.Cut(target, "#")

				// A bare "#anchor" is a link into this same page.
				resolved := filepath.Join(dir, path)
				if path == "" {
					resolved = repoPath(page)
				} else if _, err := os.Stat(resolved); err != nil {
					t.Errorf("%s links to %q, which does not exist", page, target)
					continue
				}

				if !hasFragment || fragment == "" || !strings.HasSuffix(resolved, ".md") {
					continue
				}
				raw, err := os.ReadFile(resolved)
				if err != nil {
					t.Errorf("%s links to %q but it cannot be read: %v", page, target, err)
					continue
				}
				if !anchorsOf(string(raw))[fragment] {
					t.Errorf("%s links to %q, but that document has no heading with anchor #%s",
						page, target, fragment)
				}
			}
		})
	}
}
