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
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/cli"
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
//
// One rule governs this map and the two like it below, because the suite that
// exists to prevent rot is the last place rot should be allowed to sit: an
// exemption may only name a file that is tracked in the repository. A file listed
// in .gitignore is absent from every clone and from CI, so an exemption for it can
// never fire there — it is dead configuration, and it tells a reader the file
// ships with the project when it does not. That is what the kuberecord-backlog-*
// entries were, and why they are gone; do not re-add them because a local copy of
// a backlog makes your own run red.
//
// CLAUDE.md and task.md are the deliberate exception, and the only one, in the
// maps that name them: they are gitignored, but they are the agent-workflow files,
// present in the working tree of every session that runs this suite, and they
// legitimately quote the retired names in order to record what replaced them.
// (This map names CLAUDE.md only. It previously carried a "task.txt" entry for a
// file that has never existed under that name in this repository, which is why a
// working tree holding a task.md reports it here.)
var allowedToNameBannedConfig = map[string]string{
	"CHANGELOG.md":           "the removal record and its migration table",
	"CLAUDE.md":              "the contributor guide, where D5 records what was removed",
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

// walkRepositoryText calls visit once per file worth reading as an instruction,
// with a slash-separated repository-relative path and the file's contents.
//
// It walks the *filesystem* rather than `git ls-files`, deliberately: generated
// output that shadows a source file, and an untracked file a contributor is about
// to commit, are both exactly what a tracked-files-only scan misses. That is the
// lesson the v0.1.0 rename left behind, and the reason CLAUDE.md's definition of
// done specifies `rg --no-ignore`.
func walkRepositoryText(t *testing.T, visit func(rel, content string)) {
	t.Helper()
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
		// Coverage profiles and other large build artifacts are not instructions,
		// and reading them costs more than the checks are worth.
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
		visit(rel, string(raw))
		return nil
	})
	if err != nil {
		t.Fatalf("walk repository: %v", err)
	}
}

// lineOf reports the 1-based line number of a byte offset, for a message that
// points at the occurrence rather than at the file.
func lineOf(content string, offset int) int {
	return 1 + strings.Count(content[:offset], "\n")
}

// TestNoEnvVarEraConfiguration is the grep check Task 3.4 asks for, as a test
// rather than as a command somebody has to remember to run.
func TestNoEnvVarEraConfiguration(t *testing.T) {
	walkRepositoryText(t, func(rel, content string) {
		if _, exempt := allowedToNameBannedConfig[rel]; exempt {
			return
		}
		for _, banned := range bannedConfig {
			if loc := banned.pattern.FindStringIndex(content); loc != nil {
				t.Errorf("%s:%d names %s, which Phase 1 removed with no compatibility shim (D5). It is now %s.",
					rel, lineOf(content, loc[0]), banned.name, banned.became)
			}
		}
	})
}

//
// The v0.1.0 sink reference is gone from everywhere that authors one (D10)
//

// legacySinkRefField matches the retired `sinkRef` field name and nothing else.
//
// Both word boundaries are load-bearing, because `\b` treats `_` as a word
// character but not a letter: without the trailing one this would match the type
// `SinkReference`, and without the leading one it would match `ReasonLegacySinkRef`
// and `legacySinkRefMessage` — the very symbols that exist to *report* the retired
// field, and whose whole job is to keep naming it.
var legacySinkRefField = regexp.MustCompile(`\bsinkRef\b`)

// allowedToNameLegacySinkRef are the files whose job is to say what the field
// became, or to prove that it is gone.
//
// It is the same bargain as allowedToNameBannedConfig above: "the old name appears
// nowhere" would be satisfied by deleting the migration instructions, which would
// leave an upgrading user holding a parked rule and no way to find out why. So the
// exemption is a short, explicit list rather than a pattern — adding to it is a
// decision somebody makes on purpose, and each entry says what earns it.
var allowedToNameLegacySinkRef = map[string]string{
	"CHANGELOG.md":                      "the release record and its migration steps",
	"docs/UPGRADING.md":                 "the upgrade page: it must name what to replace",
	"CLAUDE.md":                         "the contributor guide, where D10 records the rename",
	"task.md":                           "the task brief handed to the agent",
	"internal/controller/conditions.go": "the LegacySinkRef reason, documented",
	"internal/controller/streamrule_controller.go":      "the legacy guard's condition message",
	"internal/controller/streamrule_controller_test.go": "asserts that message names it",
	"internal/controller/suite_test.go":                 "stages what an upgrade leaves in etcd",
	"api/v1alpha1/crdmanifests_test.go":                 "asserts the schema does *not* contain it",
	"api/v1alpha1/streamrule_envtest_test.go":           "asserts the apiserver rejects it",
	"test/docs/docs_test.go":                            "this test",
}

// TestNoLegacySinkRefAuthoring is the `rg --no-ignore` scan Task 4.5 asks for, as
// a test.
//
// A stale `sinkRef:` in a sample, a chart template or a doc is worse than a stale
// sentence: `spec.sink` is required now, so the manifest a reader copies is
// rejected outright — and the ones a suite renders are rejected in CI, at which
// point the failure is a rule that never appeared rather than a field that was
// renamed. Every occurrence that legitimately remains is naming the field in order
// to explain what replaced it, which is what the exemption list above enumerates.
func TestNoLegacySinkRefAuthoring(t *testing.T) {
	walkRepositoryText(t, func(rel, content string) {
		if _, exempt := allowedToNameLegacySinkRef[rel]; exempt {
			return
		}
		for _, loc := range legacySinkRefField.FindAllStringIndex(content, -1) {
			t.Errorf("%s:%d names the retired `sinkRef` field, which v0.2.0 replaced with "+
				"`spec.sink {kind, name}` (D10). A rule authored against it is rejected: "+
				"`spec.sink` is required and has no name default. See docs/UPGRADING.md.",
				rel, lineOf(content, loc[0]))
		}
	})
}

// TestMigrationRecordStillNamesThem is the other half of both scans above, and
// the reason they are worth writing down: "the old names appear nowhere" would be
// satisfied by deleting the migration table, which would leave an upgrading user
// with no way to find out what their configuration became.
//
// The retired `sinkRef` field is held to the same bargain as the retired
// environment variables, and for a slightly sharper reason. CHANGELOG.md is
// exempt from TestNoLegacySinkRefAuthoring *so that* it can carry the migration
// steps; without this half, that exemption would let the steps be deleted with
// the scan still green — the removal record for a breaking change quietly
// removed, in the one file where a v0.x break is supposed to be spelled out. It
// is matched with legacySinkRefField rather than with a substring because
// `SinkReference` is a live type name a substring check would happily accept.
func TestMigrationRecordStillNamesThem(t *testing.T) {
	changelog := readFile(t, "CHANGELOG.md")
	for _, name := range []string{"WATCHED_GVKS", "CH_ADDR", "CH_DATABASE", "CH_USERNAME", "CH_PASSWORD"} {
		if !strings.Contains(changelog, name) {
			t.Errorf("CHANGELOG.md no longer names %s; the migration table is how a user finds out what it became", name)
		}
	}
	if !legacySinkRefField.MatchString(changelog) {
		t.Error("CHANGELOG.md no longer names `sinkRef`; it is exempt from the scan above so that " +
			"it can, and the migration record is how a user holding a rule authored against v0.1.0 " +
			"finds out that the field became `spec.sink {kind, name}` (D10)")
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

// TestQuickstartShowsTheOutsideTheClusterRoute keeps the ClickHouse quickstart
// from walking a first-time reader into a wall.
//
// The sink that quickstart installs records a Service DNS name, because that is
// the address the operator — which runs inside the cluster — dials. Every CLI
// command the README then suggests runs on a laptop, where that name resolves to
// nothing. The CLI diagnoses it, but a quickstart that produces a diagnosable
// failure it could have pre-empted has still spent a stranger's first ten minutes
// on a detour.
//
// So this checks the three things that make the step self-contained: the
// forwarded port, the flag that points the run at it, and the link onward for the
// reader who wants to stop passing the flag. Presence, not wording — the prose
// around them is free to be rewritten.
func TestQuickstartShowsTheOutsideTheClusterRoute(t *testing.T) {
	readme := readFile(t, "examples/quickstart/README.md")

	for _, tc := range []struct{ want, why string }{
		{
			"kubectl port-forward -n kuberecord-quickstart svc/clickhouse",
			"the forward itself, against the Service the sink names",
		},
		{
			"--sink-addr 127.0.0.1:9000",
			"the flag that points one run at the forwarded port, shown inline",
		},
		{
			"../../docs/CLI.md#running-the-cli-outside-the-cluster",
			"where a reader goes for the profile route and for why the CLI forwards nothing itself",
		},
	} {
		if !strings.Contains(readme, tc.want) {
			t.Errorf("examples/quickstart/README.md no longer shows %q — %s", tc.want, tc.why)
		}
	}

	// The section it links to has to keep the two claims a reader arrives for.
	// Both are load-bearing: the first is the reason this tool has no write verb
	// anywhere in it, and the second is the property that makes reading an
	// archive the answer to the whole problem rather than a workaround for it.
	reference := readFile(t, "docs/CLI.md")
	const heading = "## Running the CLI outside the cluster\n"
	_, section, found := strings.Cut(reference, heading)
	if !found {
		t.Fatalf("docs/CLI.md has no %q section for the quickstart to link into", strings.TrimSpace(heading))
	}
	if next := strings.Index(section, "\n## "); next >= 0 {
		section = section[:next]
	}
	for _, tc := range []struct{ want, why string }{
		{"pods/portforward", "why the CLI will not forward the port itself: it is a write verb"},
		{"--source", "that an archive read directly never meets any of this"},
	} {
		if !strings.Contains(section, tc.want) {
			t.Errorf("the \"Running the CLI outside the cluster\" section no longer mentions %q — %s",
				tc.want, tc.why)
		}
	}
}

//
// The tee example is complete, self-consistent and CI-tested (Task 7.1)
//

// teeExampleFiles is every file the tee example and docs/TEE.md promise between
// them. The acceptance criteria ask for a *runnable* example, and an example
// missing the object store it archives into, or the credentials it authenticates
// with, is not one.
var teeExampleFiles = []string{
	"examples/tee/README.md",
	"examples/tee/kustomization.yaml",
	"examples/tee/minio.yaml",
	"examples/tee/secret.yaml",
	"examples/tee/hot-sink.yaml",
	"examples/tee/cold-sink.yaml",
	"examples/tee/namespace.yaml",
	"examples/tee/rules.yaml",
	"examples/tee/workload.yaml",
	"examples/tee/bucket.sh",
	"docs/TEE.md",
}

func TestTeeExampleFilesExist(t *testing.T) {
	for _, rel := range teeExampleFiles {
		t.Run(rel, func(t *testing.T) {
			info, err := os.Stat(repoPath(rel))
			if err != nil {
				t.Fatalf("%s is promised by the tee example but missing: %v", rel, err)
			}
			if info.Size() == 0 {
				t.Fatalf("%s is empty", rel)
			}
		})
	}

	script := repoPath("examples/tee/bucket.sh")
	info, err := os.Stat(script)
	if err != nil {
		t.Fatalf("stat bucket.sh: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Error("examples/tee/bucket.sh is not executable; the README runs it directly")
	}
}

// teeKeyPair is the key-pair Secret shape both halves of the example carry.
var teeKeyPair = []string{"accessKeyId", "secretAccessKey"}

// stringDataValue matches one quoted `stringData` entry of a manifest.
func stringDataValue(key string) *regexp.Regexp {
	return regexp.MustCompile(`(?m)^\s*` + regexp.QuoteMeta(key) + `:\s*"([^"]*)"`)
}

// TestTeeExampleCredentialsAgree guards the same class of mistake
// TestQuickstartPasswordsAgree does, one backend along: the object store's root
// credentials are written down twice — once for the server, once beside the
// operator — and if the two drift, the S3Sink reports BucketReachable=False and
// archives nothing while the ClickHouse half keeps working perfectly.
//
// That is a nastier failure than an outright broken example, because a half-
// working tee looks like a working one until somebody reads the bucket.
func TestTeeExampleCredentialsAgree(t *testing.T) {
	server := readFile(t, "examples/tee/minio.yaml")
	operator := readFile(t, "examples/tee/secret.yaml")

	for _, key := range teeKeyPair {
		t.Run(key, func(t *testing.T) {
			pattern := stringDataValue(key)
			serverMatch := pattern.FindStringSubmatch(server)
			operatorMatch := pattern.FindStringSubmatch(operator)
			if serverMatch == nil {
				t.Fatalf("examples/tee/minio.yaml sets no %s", key)
			}
			if operatorMatch == nil {
				t.Fatalf("examples/tee/secret.yaml sets no %s", key)
			}
			if serverMatch[1] != operatorMatch[1] {
				t.Errorf("%s is %q in minio.yaml but %q in secret.yaml; the sink could not authenticate",
					key, serverMatch[1], operatorMatch[1])
			}
			if serverMatch[1] == "" {
				t.Errorf("%s is empty in both files", key)
			}
		})
	}
}

// TestTeeBucketScriptMatchesTheSink is the seam between the example's one
// imperative step and its declarative half.
//
// bucket.sh creates a bucket by name and the S3Sink writes to a bucket by name,
// and nothing connects the two but this check. Rename either and the example
// stands up a cluster where MinIO is healthy, the bucket exists, and the sink
// sits at BucketReachable=False pointing at a bucket that does not — which reads
// as a broken operator rather than as a typo in a shell variable.
func TestTeeBucketScriptMatchesTheSink(t *testing.T) {
	script := readFile(t, "examples/tee/bucket.sh")
	sinkSpec := readFile(t, "examples/tee/cold-sink.yaml")

	declared := regexp.MustCompile(`(?m)^\s*bucket:\s*(\S+)`).FindStringSubmatch(sinkSpec)
	if declared == nil {
		t.Fatal("examples/tee/cold-sink.yaml sets no spec.bucket")
	}
	created := regexp.MustCompile(`(?m)^BUCKET="\$\{BUCKET:-([^}]*)\}"`).FindStringSubmatch(script)
	if created == nil {
		t.Fatal("examples/tee/bucket.sh declares no default BUCKET")
	}
	if declared[1] != created[1] {
		t.Errorf("the S3Sink archives to bucket %q but bucket.sh creates %q", declared[1], created[1])
	}

	// The same story one level down: the script reads the key pair out of the
	// Secret the example's MinIO manifest ships, so a renamed Secret breaks it.
	if !strings.Contains(readFile(t, "examples/tee/minio.yaml"), "name: minio-credentials") {
		t.Error("examples/tee/minio.yaml no longer ships the Secret bucket.sh reads its credentials from")
	}
}

// teeCustomResources are the example's custom resources and the types they must
// decode into.
var teeCustomResources = []struct {
	file string
	// objects returns one empty target per document in the file, in order.
	objects func() []any
}{
	{"examples/tee/hot-sink.yaml", func() []any { return []any{&v1alpha1.ClickHouseSink{}} }},
	{"examples/tee/cold-sink.yaml", func() []any { return []any{&v1alpha1.S3Sink{}} }},
	{"examples/tee/rules.yaml", func() []any { return []any{&v1alpha1.StreamRule{}, &v1alpha1.StreamRule{}} }},
}

// TestTeeExampleCustomResourcesDecode decodes the example's CRs into the typed
// structs with unknown fields rejected.
//
// It is a cheap stand-in for the thing that would otherwise catch a typo — an
// API server — and it catches the failure that matters most in a hand-written
// example: a field name that was renamed, or never existed. A CRD prunes unknown
// fields silently rather than rejecting them, so `spec.rotation.maxObjectAg: 30s`
// would apply cleanly, default to five minutes, and leave a reader waiting for an
// object that arrives ten times later than the file says.
//
// It does not evaluate CEL, bounds or defaults; the e2e scenario applies these
// same files against a real API server, which does.
func TestTeeExampleCustomResourcesDecode(t *testing.T) {
	for _, tc := range teeCustomResources {
		t.Run(tc.file, func(t *testing.T) {
			documents := splitYAML(t, readFile(t, tc.file))
			targets := tc.objects()
			if len(documents) != len(targets) {
				t.Fatalf("%s holds %d documents, expected %d", tc.file, len(documents), len(targets))
			}
			for i, doc := range documents {
				if err := yaml.UnmarshalStrict(doc, targets[i]); err != nil {
					t.Errorf("%s document %d does not decode into %T: %v", tc.file, i+1, targets[i], err)
				}
			}
		})
	}
}

// splitYAML returns the non-empty documents of a multi-document manifest.
func splitYAML(t *testing.T, content string) [][]byte {
	t.Helper()
	reader := yaml.NewYAMLReader(bufio.NewReader(strings.NewReader(content)))
	var documents [][]byte
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return documents
		}
		if err != nil {
			t.Fatalf("split YAML: %v", err)
		}
		if len(strings.TrimSpace(string(doc))) > 0 {
			documents = append(documents, doc)
		}
	}
}

// TestTeeExampleIsCITested is the seam Task 7.1's third criterion depends on.
//
// The acceptance criterion is that CI applies *the example*, and the only thing
// making that true is one line in the e2e overlay naming examples/tee as its
// base. Copy the manifests into test/e2e/manifests instead — the obvious thing to
// do when the example is inconvenient to patch — and every assertion still
// passes while the file a reader copies is no longer tested by anything.
func TestTeeExampleIsCITested(t *testing.T) {
	overlay := readFile(t, "test/e2e/manifests/tee/kustomization.yaml")
	if !strings.Contains(overlay, "examples/tee") {
		t.Error("test/e2e/manifests/tee/kustomization.yaml no longer bases on examples/tee; " +
			"CI would be applying a copy of the example rather than the example")
	}

	// Both paths reach the scenario through constants, so these assert on the
	// constants' values rather than on the scenario body.
	suite := readFile(t, "test/e2e/e2e_suite_test.go")
	for _, want := range []string{"test/e2e/manifests/tee", "examples/tee/workload.yaml"} {
		if !strings.Contains(suite, want) {
			t.Errorf("the e2e suite no longer names %q; the tee scenario cannot be applying the example", want)
		}
	}
	if !strings.Contains(readFile(t, "test/e2e/tee_test.go"), "applyKustomization(teeOverlay)") {
		t.Error("test/e2e/tee_test.go no longer applies the tee overlay")
	}
}

// TestTeeExampleHotSinkMatchesQuickstart keeps the example's two-command promise
// honest.
//
// examples/tee/README.md tells a reader to run `make quickstart` and then apply
// this example, which only works while the ClickHouseSink here names the
// ClickHouse the quickstart actually stands up and the Secret the quickstart
// actually creates. Move either and the reader gets a sink parked at
// Unreachable — and the e2e suite would not catch it, because its overlay patches
// exactly this address.
func TestTeeExampleHotSinkMatchesQuickstart(t *testing.T) {
	hot := readFile(t, "examples/tee/hot-sink.yaml")

	addr := regexp.MustCompile(`(?m)^\s*addr:\s*(\S+)`).FindStringSubmatch(hot)
	if addr == nil {
		t.Fatal("examples/tee/hot-sink.yaml sets no spec.connection.addr")
	}
	host, _, ok := strings.Cut(addr[1], ":")
	if !ok {
		t.Fatalf("addr %q has no port", addr[1])
	}
	service, namespace, ok := strings.Cut(strings.TrimSuffix(host, ".svc"), ".")
	if !ok {
		t.Fatalf("addr %q is not a <service>.<namespace>.svc name", addr[1])
	}

	quickstart := readFile(t, "examples/quickstart/clickhouse.yaml")
	if !strings.Contains(quickstart, "name: "+namespace) {
		t.Errorf("the tee example points at namespace %q, which examples/quickstart/clickhouse.yaml does not create",
			namespace)
	}
	if !strings.Contains(quickstart, "name: "+service) {
		t.Errorf("the tee example points at Service %q, which examples/quickstart/clickhouse.yaml does not create",
			service)
	}

	secretRef := regexp.MustCompile(`credentialsSecretRef:\s*\n\s*name:\s*(\S+)`).FindStringSubmatch(hot)
	if secretRef == nil {
		t.Fatal("examples/tee/hot-sink.yaml names no credentialsSecretRef")
	}
	if !strings.Contains(readFile(t, "examples/quickstart/secret.yaml"), "name: "+secretRef[1]) {
		t.Errorf("the tee example reads Secret %q, which the quickstart does not create", secretRef[1])
	}
}

//
// Tamper-evidence: the page, and the claim it retracts (Task 7.3)
//

// retentionPageClaims is what "Tamper-evidence and retention" has to keep saying.
//
// Each entry is load-bearing rather than structural: this page exists because a
// compliance claim was stated imprecisely once, and each phrase below is a place
// where the imprecise version is the tempting one to write. The check is for
// presence and not for wording — the prose should be free to improve — but a page
// that has lost the limits half has lost the reason it was written.
var retentionPageClaims = []struct {
	want string
	why  string
}{
	{"GOVERNANCE", "the mode an operator should start with"},
	{"COMPLIANCE", "the mode nobody, including the account root, can undo"},
	{"reader-visible", "idempotency on a versioned bucket is reader-visible, not storage-level"},
	{"forward-only", "redaction cannot reach what is already archived"},
	{"delete marker", "Object Lock stops destruction, not concealment"},
	{"does not sign", "the archive is not a cryptographic chain of custody"},
	{"lifecycle", "expiration cannot remove a locked version; transitions can still run"},
}

// TestRetentionPageCoversItsSubject is the positive half of the pair below.
//
// A forbidden-token scan on its own is satisfied by a repository that says
// nothing at all about Object Lock, which is exactly the state this task was
// written to end. So the retired claim being absent only counts as progress if
// the honest replacement is present, here and in the API types a reader meets
// through `kubectl explain`.
func TestRetentionPageCoversItsSubject(t *testing.T) {
	page := readFile(t, "docs/RETENTION.md")
	for _, tc := range retentionPageClaims {
		t.Run(tc.want, func(t *testing.T) {
			if !strings.Contains(strings.ToLower(page), strings.ToLower(tc.want)) {
				t.Errorf("docs/RETENTION.md no longer says %q — %s", tc.want, tc.why)
			}
		})
	}

	// The S3ObjectLockSpec comment is the one that was wrong, and it is published
	// twice over: as Go documentation, and (for the field beside it) as the CRD
	// description `kubectl explain s3sink.spec.objectLock` prints.
	types := readFile(t, "api/v1alpha1/s3sink_types.go")
	for _, want := range []string{"reader-visible", "docs/RETENTION.md"} {
		if !strings.Contains(types, want) {
			t.Errorf("api/v1alpha1/s3sink_types.go no longer says %q; the objectLock comment is "+
				"where the retracted claim lived, and where its replacement has to stay", want)
		}
	}
	crd := readFile(t, "config/crd/bases/kuberecord.io_s3sinks.yaml")
	if !strings.Contains(crd, "docs/RETENTION.md") {
		t.Error("the generated S3Sink CRD no longer points at docs/RETENTION.md; run `make manifests`, " +
			"since the objectLock description is what kubectl explain shows")
	}
}

// retiredObjectLockClaims are the two statements about S3 Object Lock that this
// repository made and that are not true.
//
//   - A retained object refusing an overwrite. It does not: Object Lock requires
//     versioning, and on a versioned bucket a repeat PUT is accepted and creates a
//     new version. The old wording also promised the refusal would be visible in
//     the sink's logs, so it described an observable that never appears.
//   - Object Lock being enablable only when a bucket is created. AWS S3 and recent
//     MinIO both allow it on an existing versioned bucket. The claim mattered
//     because it was the stated reason `BucketIncompatible` is permanent, and that
//     argument holds without it — enabling the lock is a human's operation either
//     way.
//
// Both are scanned for rather than trusted to stay fixed, because each was
// repeated across the API types, the write path, the controller, the examples and
// the docs — nine places between them — and a claim that lives in nine places
// comes back.
var retiredObjectLockClaims = []struct {
	name    string
	pattern *regexp.Regexp
	instead string
}{
	{
		name:    "a locked object refusing an overwrite",
		pattern: regexp.MustCompile(`(?i)(reject|refus)[a-z]*\s+the\s+overwrite`),
		instead: "a versioned bucket accepts the retried PUT and keeps both versions; " +
			"the deduplication a reader sees is of current versions",
	},
	{
		name:    "Object Lock being creation-only",
		pattern: regexp.MustCompile(`(?i)only[^.\n]{0,60}(at bucket creation|when a bucket is created|at creation time)`),
		instead: "it is a bucket-level setting only a human on the account can turn on, " +
			"at creation or afterwards on a versioned bucket",
	},
}

// allowedToNameRetiredObjectLockClaims are the files whose job is to say what the
// claim was — the same bargain the two scans above strike with the migration
// record.
var allowedToNameRetiredObjectLockClaims = map[string]string{
	"docs/RETENTION.md": "the page that retracts them, and quotes what they said",
	"internal/sink/s3/awsstore/writer_minio_integration_test.go": "the test that disproves the first " +
		"claim against a real locked bucket, which has to state what it disproves",
	"task.md":                "the task brief handed to the agent",
	"test/docs/docs_test.go": "this test",
}

// TestNoRetiredObjectLockClaims is the negative half: the imprecise version of
// the WORM story must not survive anywhere that a reader would take as
// instruction.
//
// It is a repository-wide scan and not a docs-only one on purpose. The claim that
// caused this task was in a Go doc comment on an API type, which is the least
// likely place anyone rereads and the most likely place a future contributor
// copies from.
func TestNoRetiredObjectLockClaims(t *testing.T) {
	walkRepositoryText(t, func(rel, content string) {
		if _, exempt := allowedToNameRetiredObjectLockClaims[rel]; exempt {
			return
		}
		for _, claim := range retiredObjectLockClaims {
			for _, loc := range claim.pattern.FindAllStringIndex(content, -1) {
				t.Errorf("%s:%d states the retired claim that %s. It is not true: %s. See docs/RETENTION.md.",
					rel, lineOf(content, loc[0]), claim.name, claim.instead)
			}
		}
	})
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
	for _, doc := range []string{
		"docs/SCHEMA.md", "docs/RBAC.md", "docs/PERFORMANCE.md", "docs/QUERIES.md",
		// The tee page is linked from the feature list rather than only from the
		// documentation index (Task 7.1): a reader deciding whether kuberecord can
		// give them both a queryable timeline and a compliance archive is reading the
		// feature list, and the answer is a pattern rather than a setting.
		"docs/TEE.md",
		// And the retention page beside it (Task 7.3), because the same reader is the
		// one being told the archive is WORM-capable: the page that qualifies that
		// claim has to be reachable from where the claim is made.
		"docs/RETENTION.md",
	} {
		t.Run("links "+doc, func(t *testing.T) {
			if !strings.Contains(readme, "("+doc+")") {
				t.Errorf("README.md no longer links %s", doc)
			}
		})
	}

	if !strings.Contains(readme, "examples/quickstart/") {
		t.Error("README.md no longer points at examples/quickstart/")
	}
	if !strings.Contains(readme, "examples/tee/") {
		t.Error("README.md no longer points at examples/tee/, the runnable half of the tee pattern")
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
		// The community files are followed exactly like a docs/ page: both send a
		// reader onward into docs/, and a contributor or a security reporter
		// following a link that no longer resolves is the reader least likely to
		// try a second time.
		"CONTRIBUTING.md",
		"SECURITY.md",
		"examples/quickstart/README.md",
		"examples/tee/README.md",
		"examples/zero-infra/README.md",
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

//
// The CLI reference is complete, and the README leads with what the CLI prints
// (Task 12.3)
//

// commandTree returns every command of the real CLI, in the order cobra holds
// them, with the auto-generated `help` entry dropped.
//
// It builds the actual tree rather than a list of names, which is the whole point
// of the two checks below: a command or a flag added in internal/cli appears here
// the moment it is added, with no second list to keep in step. A hand-maintained
// table would document the flags somebody remembered, which is precisely the set
// that needs no reminding.
func commandTree(t *testing.T) []*cobra.Command {
	t.Helper()

	root, _ := cli.NewRootCommand("kuberecord", genericiooptions.NewTestIOStreamsDiscard())

	var walk func(*cobra.Command)
	var all []*cobra.Command
	walk = func(cmd *cobra.Command) {
		all = append(all, cmd)
		for _, child := range cmd.Commands() {
			// `help` is cobra's own, is not part of the surface this project
			// designed, and is not what a reference page is for.
			if child.Name() == "help" {
				continue
			}
			walk(child)
		}
	}
	walk(root)
	return all
}

// commandPath is a command's address without the binary name — "timeline",
// "config set-profile" — which is how docs/CLI.md refers to one.
func commandPath(root, cmd *cobra.Command) string {
	return strings.TrimPrefix(strings.TrimPrefix(cmd.CommandPath(), root.CommandPath()), " ")
}

// TestCLIReferenceDocumentsEveryCommand is Task 12.3's "every command" criterion,
// checked against the binary rather than against a memory of it.
//
// The reference is allowed to spell a command however reads best — `get --at`
// rather than `get`, `kuberecord config` rather than `config` — so several
// spellings are accepted. What is not allowed is silence: a command a user can
// type and cannot look up is a command that will be discovered by accident, and
// its flags with it.
func TestCLIReferenceDocumentsEveryCommand(t *testing.T) {
	reference := readFile(t, "docs/CLI.md")
	all := commandTree(t)
	root := all[0]

	documented := 0
	for _, cmd := range all[1:] {
		path := commandPath(root, cmd)
		t.Run(path, func(t *testing.T) {
			spellings := []string{
				"`" + path + "`",
				// `get --at`, `config set-profile …` — a backticked name that
				// carries something after it.
				"`" + path + " ",
				"`kuberecord " + path + "`",
				"`kubectl kuberecord " + path + "`",
			}
			if !slices.ContainsFunc(spellings, func(s string) bool { return strings.Contains(reference, s) }) {
				t.Errorf("docs/CLI.md never names the %q command; tried %v", path, spellings)
				return
			}
			documented++
		})
	}

	// Non-vacuity. A walk that found nothing would pass every subtest above by
	// running none of them, and a reference test that certifies an empty set is
	// worse than no test: it reports that the page is complete.
	if documented < 7 {
		t.Fatalf("the command walk found only %d documented commands; the CLI has more than that, "+
			"so this check is measuring nothing", documented)
	}
}

// TestCLIReferenceDocumentsEveryFlag is the "every flag" half.
//
// It includes the flags kuberecord inherits from genericclioptions.ConfigFlags,
// deliberately and at the cost of a little churn: docs/CLI.md enumerates that set
// under "Inherited from kubectl", so a client-go bump that adds one to the tree
// leaves the page's list wrong, and a page that claims to list them all is worth
// only as much as that claim. Adding the name to the list is the fix, and it is a
// one-line one.
func TestCLIReferenceDocumentsEveryFlag(t *testing.T) {
	reference := readFile(t, "docs/CLI.md")
	all := commandTree(t)
	root := all[0]

	// Collected across the whole tree first, because the same flag is reachable
	// from several commands and one undocumented name should be one failure.
	owners := map[string][]string{}
	collect := func(cmd *cobra.Command, set *pflag.FlagSet) {
		set.VisitAll(func(f *pflag.Flag) {
			path := commandPath(root, cmd)
			if path == "" {
				path = "(global)"
			}
			if !slices.Contains(owners[f.Name], path) {
				owners[f.Name] = append(owners[f.Name], path)
			}
		})
	}
	for _, cmd := range all {
		collect(cmd, cmd.LocalFlags())
		collect(cmd, cmd.PersistentFlags())
	}

	names := make([]string, 0, len(owners))
	for name := range owners {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(reference, "--"+name) {
				t.Errorf("docs/CLI.md never names --%s, which %s accepts",
					name, strings.Join(owners[name], ", "))
			}
		})
	}

	// Non-vacuity, as above. The tree carries well over forty flags; a run that
	// found a handful has walked something other than the CLI.
	if len(names) < 40 {
		t.Fatalf("the flag walk found only %d flags (%v); the CLI has far more, "+
			"so this check is measuring nothing", len(names), names)
	}
}

// fencedBlocks returns every fenced code block in a Markdown document, paired
// with the 1-based line its opening fence sits on.
func fencedBlocks(content string) []struct {
	Line int
	Body string
} {
	var out []struct {
		Line int
		Body string
	}
	lines := strings.Split(content, "\n")
	for i := 0; i < len(lines); i++ {
		if !strings.HasPrefix(lines[i], "```") {
			continue
		}
		start := i
		i++
		var body []string
		for i < len(lines) && !strings.HasPrefix(lines[i], "```") {
			body = append(body, lines[i])
			i++
		}
		out = append(out, struct {
			Line int
			Body string
		}{start + 1, strings.Join(body, "\n")})
	}
	return out
}

// headingLineNumber returns the 1-based line an exact heading sits on, or 0.
func headingLineNumber(content, heading string) int {
	for i, line := range strings.Split(content, "\n") {
		if strings.TrimSpace(line) == heading {
			return i + 1
		}
	}
	return 0
}

// firstScreenful is how many lines of README.md count as "the first screen" for
// the check below.
//
// It is a budget rather than a measurement: nobody's terminal is exactly this
// tall, and the number exists so that the ordering requirement cannot be met by a
// timeline block that is technically before the installation section and
// practically three scrolls down.
const firstScreenful = 45

// TestREADMELeadsWithTheTimeline is Task 12.3's central criterion.
//
// The README's job in its first screen is to show what the tool prints. It used
// to lead with an architecture description, which answers a question nobody has
// yet: a stranger deciding whether to keep reading wants the output, and the 3am
// incident — the change, the actor, and the Event it caused — is the output that
// makes the case in one screen.
//
// The check is structural rather than about wording. Prose should be free to
// improve; what must not drift is the ordering, because every restructure of a
// README is an opportunity for the flagship block to sink below the fold.
func TestREADMELeadsWithTheTimeline(t *testing.T) {
	readme := readFile(t, "README.md")

	var flagship struct {
		Line int
		Body string
	}
	for _, block := range fencedBlocks(readme) {
		if strings.Contains(block.Body, "kuberecord timeline") {
			flagship = block
			break
		}
	}
	if flagship.Line == 0 {
		t.Fatal("README.md contains no code block showing `kuberecord timeline` output; " +
			"the flagship output is what the first screen is for")
	}

	if flagship.Line > firstScreenful {
		t.Errorf("the timeline block starts on line %d, past the %d-line first screenful",
			flagship.Line, firstScreenful)
	}

	// Before installation and before architecture, which is the ordering the
	// criterion names. A section that is absent scores 0 and cannot fail this.
	for _, heading := range []string{"## Installing", "## Installing the CLI", "## Architecture", "## Why"} {
		if line := headingLineNumber(readme, heading); line != 0 && line < flagship.Line {
			t.Errorf("%q is on line %d, above the timeline block on line %d", heading, line, flagship.Line)
		}
	}

	// The 3am framing is three facts in one table, and a block that has lost one
	// of them has lost the argument it was making.
	for _, want := range []struct{ substring, why string }{
		{"--with-events", "the correlated Kubernetes Event is half the story"},
		{"Modified", "the change"},
		{"Event", "the Event it caused"},
		{"→", "an old value beside a new one, which is what makes a row an answer"},
	} {
		if !strings.Contains(flagship.Body, want.substring) {
			t.Errorf("the timeline block no longer shows %q — %s", want.substring, want.why)
		}
	}

	// And the actor column, which is the "who" the tagline promises.
	if !strings.Contains(flagship.Body, "ACTOR") {
		t.Error("the timeline block no longer shows the ACTOR column; " +
			"\"git blame for your cluster\" is a claim about who, not only about what")
	}
}

//
// The zero-infrastructure quickstart (Task 12.3)
//

var zeroInfraFiles = []string{
	"examples/zero-infra/README.md",
	"examples/zero-infra/kind.yaml",
	"examples/zero-infra/minio.yaml",
	"examples/zero-infra/secret.yaml",
	"examples/zero-infra/sink.yaml",
	"examples/zero-infra/rule.yaml",
	"examples/zero-infra/demo.yaml",
	"examples/zero-infra/zero-infra.sh",
}

func TestZeroInfraFilesExist(t *testing.T) {
	for _, rel := range zeroInfraFiles {
		t.Run(rel, func(t *testing.T) {
			info, err := os.Stat(repoPath(rel))
			if err != nil {
				t.Fatalf("%s is promised by the zero-infrastructure quickstart but missing: %v", rel, err)
			}
			if info.Size() == 0 {
				t.Fatalf("%s is empty", rel)
			}
		})
	}

	script := repoPath("examples/zero-infra/zero-infra.sh")
	info, err := os.Stat(script)
	if err != nil {
		t.Fatalf("stat zero-infra.sh: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Errorf("examples/zero-infra/zero-infra.sh is not executable; " +
			"`make quickstart-zero-infra` runs it directly")
	}
}

// TestZeroInfraIsWhatItClaims pins the two halves of the criterion that make this
// a *different* path rather than a second copy of the first one: it installs with
// helm, it reads back with the CLI, and there is no ClickHouse in it anywhere.
//
// The last of those is the one worth a test. "No infrastructure" is easy to lose
// by accident — a debugging session that adds a ClickHouse to compare against, a
// copied block from the other quickstart — and the resulting script would still
// pass every other check here while no longer demonstrating the thing it exists
// to demonstrate.
func TestZeroInfraIsWhatItClaims(t *testing.T) {
	script := readFile(t, "examples/zero-infra/zero-infra.sh")

	for _, want := range []struct{ substring, why string }{
		{"upgrade --install", "the criterion is a `helm install`, not a kustomize apply"},
		{"timeline", "the path ends in an answered `kuberecord timeline`"},
		{"go build", "the CLI is built from this clone, so the run tests this tree"},
		{"ZERO_INFRA_BUDGET_SECONDS", "the ten-minute claim is enforced by a budget, not asserted"},
	} {
		if !strings.Contains(script, want.substring) {
			t.Errorf("examples/zero-infra/zero-infra.sh no longer contains %q — %s", want.substring, want.why)
		}
	}

	// No database, in the script or in anything it applies. Prose may name
	// ClickHouse — the example's README contrasts the two paths on purpose — so
	// this checks what is executed and what is applied, not what is explained.
	for _, rel := range []string{
		"examples/zero-infra/zero-infra.sh",
		"examples/zero-infra/sink.yaml",
		"examples/zero-infra/rule.yaml",
		"examples/zero-infra/minio.yaml",
		"examples/zero-infra/demo.yaml",
		"examples/zero-infra/secret.yaml",
	} {
		content := readFile(t, rel)
		for _, banned := range []string{"kind: ClickHouseSink", "clickhouse-client", "clickhouse/clickhouse-server"} {
			if strings.Contains(content, banned) {
				t.Errorf("%s contains %q; the zero-infrastructure path must stand up no database",
					rel, banned)
			}
		}
	}
}

// TestZeroInfraIsCITested is the seam the "CI-tested end to end like the existing
// quickstart, and asserted to complete in under ten minutes" criterion hangs on.
//
// Nothing but these three lines connects the committed script, the make target a
// reader is told to run, and the job that runs it on every push with a budget.
// Break any of them and the example still looks tested.
func TestZeroInfraIsCITested(t *testing.T) {
	makefile := readFile(t, "Makefile")
	for _, want := range []string{
		"examples/zero-infra/zero-infra.sh",
		"quickstart-zero-infra:",
		"quickstart-zero-infra-down:",
	} {
		if !strings.Contains(makefile, want) {
			t.Errorf("the Makefile no longer contains %q; the documented command does not exist", want)
		}
	}

	workflow := readFile(t, ".github/workflows/quickstart.yml")
	if !strings.Contains(workflow, "make quickstart-zero-infra") {
		t.Error(".github/workflows/quickstart.yml no longer runs the zero-infrastructure quickstart")
	}
	if !strings.Contains(workflow, "make quickstart-zero-infra-down") {
		t.Error(".github/workflows/quickstart.yml no longer tears the zero-infrastructure cluster down; " +
			"a failed run would leave a kind cluster on the runner")
	}

	// The budget is the ten-minute claim. A job that runs the script without one
	// tests that it works, not that it works in time — and the README promises
	// both.
	budget := regexp.MustCompile(`ZERO_INFRA_BUDGET_SECONDS:\s*"?(\d+)"?`).FindStringSubmatch(workflow)
	if budget == nil {
		t.Fatal(".github/workflows/quickstart.yml sets no ZERO_INFRA_BUDGET_SECONDS; " +
			"the under-ten-minutes claim is then untested")
	}
	if budget[1] != "600" {
		t.Errorf("the CI budget is %ss, but the README promises under ten minutes", budget[1])
	}
}

// TestZeroInfraCredentialsAgree is TestTeeExampleCredentialsAgree's twin, for the
// same failure and the same reason: the object store's root credentials are
// written down twice, and if the two drift the S3Sink reports
// BucketReachable=False and archives nothing — leaving a reader with a healthy
// operator, a healthy MinIO, and an empty bucket.
func TestZeroInfraCredentialsAgree(t *testing.T) {
	server := readFile(t, "examples/zero-infra/minio.yaml")
	operator := readFile(t, "examples/zero-infra/secret.yaml")

	for _, key := range teeKeyPair {
		t.Run(key, func(t *testing.T) {
			pattern := stringDataValue(key)
			serverMatch := pattern.FindStringSubmatch(server)
			operatorMatch := pattern.FindStringSubmatch(operator)
			if serverMatch == nil {
				t.Fatalf("examples/zero-infra/minio.yaml sets no %s", key)
			}
			if operatorMatch == nil {
				t.Fatalf("examples/zero-infra/secret.yaml sets no %s", key)
			}
			if serverMatch[1] != operatorMatch[1] {
				t.Errorf("%s is %q in minio.yaml but %q in secret.yaml; the sink could not authenticate",
					key, serverMatch[1], operatorMatch[1])
			}
			if serverMatch[1] == "" {
				t.Errorf("%s is empty in both files", key)
			}
		})
	}

	// And the third copy, which the tee example does not have: the CLI reads the
	// archive back with the same key pair, exported into the AWS credential chain
	// by the script. A drift here fails at the last step of the run, after
	// everything else has worked.
	script := readFile(t, "examples/zero-infra/zero-infra.sh")
	for _, pair := range []struct{ key, variable string }{
		{"accessKeyId", "AWS_ACCESS_KEY_ID"},
		{"secretAccessKey", "AWS_SECRET_ACCESS_KEY"},
	} {
		want := stringDataValue(pair.key).FindStringSubmatch(server)
		if want == nil {
			t.Fatalf("examples/zero-infra/minio.yaml sets no %s", pair.key)
		}
		exported := regexp.MustCompile(`export ` + pair.variable + `="([^"]*)"`).FindStringSubmatch(script)
		if exported == nil {
			t.Fatalf("zero-infra.sh exports no %s; the CLI could not read the archive", pair.variable)
		}
		if exported[1] != want[1] {
			t.Errorf("%s is %q in minio.yaml but the script exports %s=%q",
				pair.key, want[1], pair.variable, exported[1])
		}
	}
}

// TestZeroInfraSinkMatchesItsObjectStore is the seam between the declarative half
// and the one imperative step, as TestTeeBucketScriptMatchesTheSink is for the
// tee example — plus the two joins that example does not have, because there the
// archive is only written and here it is also read.
//
// Four names have to agree, and nothing but this connects them: the bucket the
// script creates and the sink writes to, the endpoint the sink dials and the
// Service that answers it, the prefix the sink writes under and the `--source`
// the CLI reads, and the cluster identity the helm install stamps and the
// standalone read asks for.
func TestZeroInfraSinkMatchesItsObjectStore(t *testing.T) {
	script := readFile(t, "examples/zero-infra/zero-infra.sh")
	sinkSpec := readFile(t, "examples/zero-infra/sink.yaml")
	minio := readFile(t, "examples/zero-infra/minio.yaml")

	shellDefault := func(name string) string {
		m := regexp.MustCompile(`(?m)^` + name + `="([^"]*)"`).FindStringSubmatch(script)
		if m == nil {
			return ""
		}
		return m[1]
	}
	yamlField := func(content, field string) string {
		m := regexp.MustCompile(`(?m)^\s*` + field + `:\s*(\S+)`).FindStringSubmatch(content)
		if m == nil {
			return ""
		}
		return m[1]
	}

	bucket := yamlField(sinkSpec, "bucket")
	if bucket == "" {
		t.Fatal("examples/zero-infra/sink.yaml sets no spec.bucket")
	}
	if got := shellDefault("BUCKET"); got != bucket {
		t.Errorf("the S3Sink archives to bucket %q but the script creates %q", bucket, got)
	}

	prefix := yamlField(sinkSpec, "prefix")
	if prefix == "" {
		t.Fatal("examples/zero-infra/sink.yaml sets no spec.prefix")
	}
	if got := shellDefault("PREFIX"); got != prefix {
		t.Errorf("the S3Sink writes under prefix %q but the CLI reads %q", prefix, got)
	}

	// The endpoint is a Service DNS name, and the Service has to exist.
	endpoint := yamlField(sinkSpec, "endpoint")
	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	host, _, _ = strings.Cut(host, ":")
	service, namespace, ok := strings.Cut(strings.TrimSuffix(host, ".svc"), ".")
	if !ok {
		t.Fatalf("spec.endpoint %q is not a <service>.<namespace>.svc name", endpoint)
	}
	if !strings.Contains(minio, "name: "+namespace) {
		t.Errorf("the sink dials namespace %q, which examples/zero-infra/minio.yaml does not create", namespace)
	}
	if !strings.Contains(minio, "name: "+service) {
		t.Errorf("the sink dials Service %q, which examples/zero-infra/minio.yaml does not create", service)
	}
	if got := shellDefault("MINIO_NAMESPACE"); got != namespace {
		t.Errorf("the sink dials namespace %q but the script execs into %q", namespace, got)
	}

	// The cluster identity is stamped by the helm install and asked for by the
	// standalone read, and there is no cluster in that step to reconcile the two.
	clusterID := shellDefault("CLUSTER_ID")
	if clusterID == "" {
		t.Fatal("zero-infra.sh declares no CLUSTER_ID")
	}
	if !strings.Contains(script, `--set clusterID="${CLUSTER_ID}"`) {
		t.Error("zero-infra.sh no longer installs the chart with the CLUSTER_ID it later queries by")
	}
	if !strings.Contains(script, `--cluster-id "${CLUSTER_ID}"`) {
		t.Error("zero-infra.sh no longer reads the archive with the cluster identity it stamped")
	}

	// And the Secret the script reads the key pair out of.
	if !strings.Contains(minio, "name: minio-credentials") {
		t.Error("examples/zero-infra/minio.yaml no longer ships the Secret the script reads its credentials from")
	}
}

// zeroInfraCustomResources are the example's custom resources and the types they
// must decode into. See TestTeeExampleCustomResourcesDecode for why a decode is
// worth doing at all: a CRD prunes an unknown field silently, so a typo in a
// hand-written example applies cleanly and behaves as the default.
var zeroInfraCustomResources = []struct {
	file    string
	objects func() []any
}{
	{"examples/zero-infra/sink.yaml", func() []any { return []any{&v1alpha1.S3Sink{}} }},
	{"examples/zero-infra/rule.yaml", func() []any { return []any{&v1alpha1.ClusterStreamRule{}} }},
}

func TestZeroInfraCustomResourcesDecode(t *testing.T) {
	for _, tc := range zeroInfraCustomResources {
		t.Run(tc.file, func(t *testing.T) {
			documents := splitYAML(t, readFile(t, tc.file))
			targets := tc.objects()
			if len(documents) != len(targets) {
				t.Fatalf("%s holds %d documents, expected %d", tc.file, len(documents), len(targets))
			}
			for i, doc := range documents {
				if err := yaml.UnmarshalStrict(doc, targets[i]); err != nil {
					t.Errorf("%s document %d does not decode into %T: %v", tc.file, i+1, targets[i], err)
				}
			}
		})
	}
}

// TestREADMEOffersTheZeroInfraPath keeps the path reachable from where somebody
// deciding whether to try kuberecord is standing. An evaluation path nobody is
// told about is a test fixture.
func TestREADMEOffersTheZeroInfraPath(t *testing.T) {
	readme := readFile(t, "README.md")
	for _, want := range []struct{ substring, why string }{
		{"make quickstart-zero-infra", "the one command the path is"},
		{"examples/zero-infra/", "where the steps are documented"},
		{"helm install", "the criterion names helm, and the reader should see which install this is"},
	} {
		if !strings.Contains(readme, want.substring) {
			t.Errorf("README.md no longer contains %q — %s", want.substring, want.why)
		}
	}
}

//
// The query library and the CLI are complements, and say so (Task 12.3)
//

// TestQueriesRoutesBetweenTheCLIAndSQL keeps docs/QUERIES.md honest about what it
// is now for.
//
// Before the CLI existed, this page was the only way to ask any of these
// questions and the reader had no choice to make. Now there is one, and the page
// that answers a single-object question in twenty lines of SQL without mentioning
// the one-line command is not merely incomplete — it is actively sending people
// the long way round.
func TestQueriesRoutesBetweenTheCLIAndSQL(t *testing.T) {
	queries := readFile(t, "docs/QUERIES.md")

	if !strings.Contains(queries, "\n## CLI or SQL?\n") {
		t.Fatal("docs/QUERIES.md has no \"CLI or SQL?\" section; the two are complements and the doc must say which is which")
	}

	// Every command the routing table is supposed to route to.
	for _, command := range []string{"timeline", "diff", "get", "blame", "scopes"} {
		if !strings.Contains(queries, "kuberecord "+command) {
			t.Errorf("docs/QUERIES.md never names `kuberecord %s`", command)
		}
	}

	// And the single-object sections each have to name the command that answers
	// them, in the section itself — a reader who arrived by anchor or by search
	// never sees the routing table at the top.
	sections := map[string]string{}
	var current string
	for line := range strings.SplitSeq(queries, "\n") {
		if strings.HasPrefix(line, "## ") || strings.HasPrefix(line, "### ") {
			current = strings.TrimSpace(strings.TrimLeft(line, "# "))
			continue
		}
		if current != "" {
			sections[current] += line + "\n"
		}
	}
	for _, heading := range []string{
		"Reconstructing an object's state at an instant",
		"What did this deleted object look like?",
		"Events for object X around time T",
		"Was anybody watching?",
		"One object's timeline",
	} {
		body, ok := sections[heading]
		if !ok {
			t.Errorf("docs/QUERIES.md no longer has a %q section", heading)
			continue
		}
		if !strings.Contains(body, "kuberecord ") {
			t.Errorf("the %q section answers a single-object question but never names the CLI command that does it in one line",
				heading)
		}
	}

	// The converse, which is the half that stops this from becoming a page that
	// deprecates itself: the wide questions have to say they stay in SQL.
	staysSQL := 0
	for _, heading := range []string{
		"Incident window — everything that changed in a namespace",
		"Drift your GitOps controller did not cause",
		"Top flappers",
	} {
		if strings.Contains(sections[heading], "stays SQL") {
			staysSQL++
		}
	}
	if staysSQL < 3 {
		t.Errorf("only %d of the three fleet-level sections say they stay in SQL; "+
			"a query library that does not say what it is still for reads as a deprecated one", staysSQL)
	}
}

// TestCLIReferenceCoversItsRequiredSubjects is the rest of Task 12.3's first
// criterion — the parts of the reference that are not a command or a flag, and
// that a page can quietly lose while still looking complete.
//
// Presence, not wording. Each entry is a subject a reader arrives needing and
// cannot get from `--help`.
func TestCLIReferenceCoversItsRequiredSubjects(t *testing.T) {
	reference := readFile(t, "docs/CLI.md")

	for _, tc := range []struct{ want, why string }{
		{"\n## Global flags\n", "the flags every command carries, and which of them are kubectl's"},
		{"\n## Output formats\n", "which command renders which format, and what a refusal means"},
		{"\n## Exit codes\n", "the four codes, and why 3 is the one worth scripting against"},
		{"cli.kuberecord.io/v1alpha1", "the structured output contract (D19)"},
		{"\n## Backend capability differences\n", "what each backend can and cannot answer (D17)"},
		{"\n### Cold scans\n", "what a scan of an unindexed archive costs, which is the trade D18 buys"},
		{"\n## Running the CLI outside the cluster\n",
			"the route out of a discovered address that only resolves inside a cluster, " +
				"which is the first failure a new user meets"},
		{"\n## The configuration file\n", "the file schema, including the fields it refuses by name"},
		{"\n### The schema, field by field\n", "a reference needs the fields, not only an annotated example"},
	} {
		if !strings.Contains(reference, tc.want) {
			t.Errorf("docs/CLI.md no longer contains %q — %s", tc.want, tc.why)
		}
	}

	// The capability table has to name every field of the declared contract. A
	// backend that gains one and a page that does not mention it is exactly the
	// silent degradation Invariant 4 exists to prevent.
	for _, capability := range []string{"deletions", "server_side_filter", "point_query", "time_bound_required"} {
		if !strings.Contains(reference, capability) {
			t.Errorf("docs/CLI.md never names the %q capability, which changes what the output means",
				capability)
		}
	}

	// The four exit codes, each spelled as the page's table spells them.
	for _, code := range []string{"| `0` |", "| `1` |", "| `2` |", "| `3` |"} {
		if !strings.Contains(reference, code) {
			t.Errorf("docs/CLI.md's exit-code table no longer has a row %s", code)
		}
	}

	// And the install channels, in the order Task 12.2 requires.
	channels := []string{"kubectl krew install kuberecord", "brew install", "releases/download", "go install"}
	at := 0
	for _, channel := range channels {
		index := strings.Index(reference[at:], channel)
		if index < 0 {
			t.Fatalf("docs/CLI.md no longer shows %q, or shows it out of order (expected order: %v)",
				channel, channels)
		}
		at += index + len(channel)
	}
}

// TestZeroInfraExampleIsLinked keeps the new page reachable. An orphan under
// examples/ is a page only its author will ever open.
func TestZeroInfraExampleIsLinked(t *testing.T) {
	for _, page := range []string{"README.md", "docs/CLI.md"} {
		if !strings.Contains(readFile(t, page), "examples/zero-infra/") {
			t.Errorf("%s does not link examples/zero-infra/", page)
		}
	}
}
