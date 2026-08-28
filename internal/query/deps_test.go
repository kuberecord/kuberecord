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

package query_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// operatorPackages are the packages the read plane must never be able to reach.
//
// The read plane is a client of the frozen schema, not of the operator's runtime.
// Coupling the two would make every refactor of the write path a release of the
// query path, and would put read-plane concerns onto the dependency graph of the
// hot path — the specific outcome the decision to keep this contract separate from
// the sink's own reader was taken to avoid.
var operatorPackages = []string{
	"github.com/kuberecord/kuberecord/internal/sink",
	"github.com/kuberecord/kuberecord/internal/pipeline",
	"github.com/kuberecord/kuberecord/internal/watch",
	"github.com/kuberecord/kuberecord/internal/controller",
}

// TestNoOperatorPackagesInTransitiveDeps asserts the boundary over the *whole*
// dependency closure, not just this package's own import block.
//
// Transitive is the interesting part. A direct import is obvious in review; what
// this catches is the reach that arrives through an innocent-looking helper
// package added later, which is how an import boundary is actually lost.
func TestNoOperatorPackagesInTransitiveDeps(t *testing.T) {
	for _, dep := range goList(t, "-deps", ".") {
		for _, forbidden := range operatorPackages {
			if dep == forbidden || strings.HasPrefix(dep, forbidden+"/") {
				t.Errorf("internal/query reaches %q (via the closure of its imports); "+
					"the read plane must not depend on the operator's runtime", dep)
			}
		}
	}
}

// patchLibrary is the one third-party dependency the contract itself carries.
//
// Replay is the single definition of the reconstruction procedure, so the
// contract owns the application of an RFC 6902 patch rather than delegating it to
// each backend. That has to be one library: two backends applying patches with
// two implementations would be two readings of the same spec, differing on
// pointer escaping and on the null-versus-absent distinction, and the divergence
// would show up as a reconstruction that is plausible and wrong rather than as an
// error. The cost is this allowance, and it is deliberately spelled as one exact
// path — a second entry here is the review conversation this shape exists to
// force.
const patchLibrary = "github.com/evanphx/json-patch/v5"

// TestDirectImportsAreStdlibOnly holds the tighter budget: what this package
// itself is allowed to write down.
//
// The transitive test above cannot express it, because apimachinery's own closure
// is large and is not ours to police. This one constrains the import block, which
// is the thing under review. apimachinery is permitted but currently unused —
// nothing in a contract made of strings, times and maps genuinely needs it, and
// listing it here rather than discovering the need later keeps the allowance a
// deliberate one instead of an argument had under pressure.
func TestDirectImportsAreStdlibOnly(t *testing.T) {
	imports := goList(t, "-f", `{{range .Imports}}{{println .}}{{end}}`, ".")

	for _, path := range imports {
		switch {
		case !strings.Contains(firstSegment(path), "."):
			// A standard-library path never has a dot in its first segment; this is
			// the same test the go command itself applies.
		case path == "k8s.io/apimachinery" ||
			strings.HasPrefix(path, "k8s.io/apimachinery/"):
		case path == patchLibrary:
		default:
			t.Errorf("internal/query imports %q: the read-plane contract may import "+
				"only the standard library, plus k8s.io/apimachinery where a "+
				"Kubernetes type is genuinely needed and %s for the patch application "+
				"the reconstruction procedure owns", path, patchLibrary)
		}
	}
}

// TestThePatchLibraryIsActuallyImported keeps the allowance above from outliving
// its reason.
//
// An exception nobody uses is an exception nobody re-argues, and this one exists
// only for as long as the contract itself applies patches. If Replay moved out or
// changed library, the allowance would silently keep the door open for the next
// import that wanted it.
func TestThePatchLibraryIsActuallyImported(t *testing.T) {
	imports := goList(t, "-f", `{{range .Imports}}{{println .}}{{end}}`, ".")
	if !slices.Contains(imports, patchLibrary) {
		t.Errorf("internal/query no longer imports %s, so the allowance in "+
			"TestDirectImportsAreStdlibOnly is now an open door rather than a stated "+
			"exception: remove it with the import", patchLibrary)
	}
}

// backendNames matches any storage technology this project has shipped, planned or
// documented as an escape hatch.
//
// The contract is what every backend is measured against, so naming one inside it
// is how the contract stops being a contract and becomes a description of whichever
// implementation was written first. That is a drift nobody notices in review — a
// comment saying "the columnar backend does X" reads as helpful — which is why it
// is a test rather than a convention.
var backendNames = regexp.MustCompile(
	`(?i)\b(clickhouse|minio|s3|postgres(ql)?|elasticsearch|duckdb|kafka)\b`)

// TestNoBackendIsNamed scans the package's own source for those names.
//
// Test files are excluded, and this file is why: it has to spell the names in order
// to look for them. The property under review is what the *contract* says, so the
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
// only the go command knows the closure — and it is accurate about build
// constraints in a way an import-block parse is not.
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
