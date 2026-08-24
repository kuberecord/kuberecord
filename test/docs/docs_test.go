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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
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
	"CHANGELOG.md":               "the removal record and its migration table",
	"kuberecord-backlog-v0.1.md": "the roadmap that specified the removal",
	"kuberecord-backlog-v0.2.md": "the roadmap that carries it forward",
	"CLAUDE.md":                  "the contributor guide, where D5 records what was removed",
	"task.txt":                   "the task brief handed to the agent",
	"test/docs/docs_test.go":     "this test",
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
	"CHANGELOG.md":                                      "the release record and its migration steps",
	"docs/UPGRADING.md":                                 "the upgrade page: it must name what to replace",
	"CLAUDE.md":                                         "the contributor guide, where D10 records the rename",
	"kuberecord-backlog-v0.1.md":                        "the roadmap that specified the field",
	"kuberecord-backlog-v0.2.md":                        "the roadmap that specified its removal",
	"task.md":                                           "the task brief handed to the agent",
	"internal/controller/conditions.go":                 "the LegacySinkRef reason, documented",
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
	"kuberecord-backlog-v0.2.md": "the roadmap that specified the correction",
	"task.md":                    "the task brief handed to the agent",
	"test/docs/docs_test.go":     "this test",
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
		"examples/quickstart/README.md",
		"examples/tee/README.md",
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
