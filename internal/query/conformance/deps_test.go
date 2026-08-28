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

package conformance

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// contractPackage is the one non-standard import this suite is allowed.
//
// A conformance suite is the contract in executable form, so what it may depend on
// is the contract and nothing else. Reaching further would make it a description of
// whichever backend it borrowed from, which is the precise failure a gate exists to
// prevent (D11).
const contractPackage = "github.com/kuberecord/kuberecord/internal/query"

// operatorPackages are the packages neither the read plane nor its suite may reach.
//
// internal/sink is on the list for a reason distinct from the other three. It is not
// merely the write path: it has a read half of its own, and a suite that could see
// it would be tempted to measure a query backend against the operator's own reader —
// two contracts with opposite pressures, one of which is on the hot path's
// dependency graph (D16).
var operatorPackages = []string{
	"github.com/kuberecord/kuberecord/internal/sink",
	"github.com/kuberecord/kuberecord/internal/pipeline",
	"github.com/kuberecord/kuberecord/internal/watch",
	"github.com/kuberecord/kuberecord/internal/controller",
}

// TestNoOperatorPackagesInTransitiveDeps asserts the boundary over the whole
// dependency closure, not just this package's own import block.
//
// Transitive is the interesting part. A direct import is obvious in review; what this
// catches is the reach that arrives through an innocent-looking helper package added
// later, which is how an import boundary is actually lost.
func TestNoOperatorPackagesInTransitiveDeps(t *testing.T) {
	for _, dep := range goList(t, "-deps", ".") {
		for _, forbidden := range operatorPackages {
			if dep == forbidden || strings.HasPrefix(dep, forbidden+"/") {
				t.Errorf("the query conformance suite reaches %q (via the closure of its imports); it "+
					"must be measurable against the contract alone", dep)
			}
		}
	}
}

// TestDirectImportsAreStdlibAndContractOnly holds the tighter budget: what this
// package itself is allowed to write down, in its sources and in its own tests
// alike.
//
// The test files are in scope deliberately. They carry the compliant engine and the
// breaking fixtures — the whole of the non-vacuity argument — and a third-party
// helper there would put the suite's credibility on something the contract does not
// mention. It is also why the leak check counts goroutines by hand rather than
// importing a leak detector.
func TestDirectImportsAreStdlibAndContractOnly(t *testing.T) {
	format := `{{range .Imports}}{{println .}}{{end}}{{range .TestImports}}{{println .}}{{end}}` +
		`{{range .XTestImports}}{{println .}}{{end}}`
	for _, path := range goList(t, "-f", format, ".") {
		switch {
		case !strings.Contains(firstSegment(path), "."):
			// A standard-library path never has a dot in its first segment; this is
			// the same test the go command itself applies.
		case path == contractPackage:
		default:
			t.Errorf("the query conformance suite imports %q: it may import only the standard library "+
				"and %s", path, contractPackage)
		}
	}
}

// backendNames matches any storage technology this project has shipped, planned or
// documented as an escape hatch.
//
// The suite is what every backend is measured against, so naming one inside it is how
// it stops being a suite and becomes a description of whichever implementation was
// written first. That is a drift nobody notices in review — a comment saying "the
// columnar backend does X" reads as helpful — which is why it is a test rather than a
// convention.
var backendNames = regexp.MustCompile(
	`(?i)\b(clickhouse|minio|s3|postgres(ql)?|elasticsearch|duckdb|kafka)\b`)

// TestNoBackendIsNamed scans the package's own source for those names.
//
// Test files are excluded, and this file is why: it has to spell the names in order
// to look for them. The property under review is what the *suite* says, so the
// non-test sources are exactly the right scope.
func TestNoBackendIsNamed(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for line := range strings.SplitSeq(string(source), "\n") {
			if match := backendNames.FindString(line); match != "" {
				t.Errorf("%s names the backend %q: %s", name, match, strings.TrimSpace(line))
			}
		}
	}
}

// goList runs the go command in the package directory and returns its non-empty
// output lines.
//
// Shelling out rather than parsing the source is what the transitive check needs —
// only the go command knows the closure — and it is accurate about build constraints
// in a way an import-block parse is not.
func goList(t *testing.T, args ...string) []string {
	t.Helper()

	goBin, err := exec.LookPath("go")
	if err != nil {
		// A Go test running without the go command on PATH is possible (a test
		// binary shipped to a bare machine) and is not this package's failure.
		t.Skipf("the go command is not on PATH, so the dependency budget cannot be checked: %v", err)
	}

	cmd := exec.Command(goBin, append([]string{"list"}, args...)...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v\n%s", strings.Join(args, " "), err, stderr.String())
	}

	var lines []string
	for line := range strings.SplitSeq(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// firstSegment returns the part of an import path before the first slash.
func firstSegment(path string) string {
	segment, _, _ := strings.Cut(path, "/")
	return segment
}
