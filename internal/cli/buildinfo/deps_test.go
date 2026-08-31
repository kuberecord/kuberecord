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

package buildinfo_test

import (
	"os/exec"
	"strings"
	"testing"
)

// operatorPackages are the packages the CLI must never be able to reach (D20).
//
// They are listed here, as they are in every other CLI package, even though the
// stronger assertion below already forbids them along with everything else that
// is not the standard library. The list is what somebody grepping for the
// boundary finds, and a package that quietly dropped off the enforcement while
// still naming the rule would be worse than one that never claimed it.
var operatorPackages = []string{
	"github.com/kuberecord/kuberecord/internal/pipeline",
	"github.com/kuberecord/kuberecord/internal/watch",
	"github.com/kuberecord/kuberecord/internal/controller",
	"github.com/kuberecord/kuberecord/internal/sink",
}

// TestNoOperatorPackagesInTransitiveDeps asserts the boundary over the whole
// dependency closure, not just this package's own import block.
func TestNoOperatorPackagesInTransitiveDeps(t *testing.T) {
	for _, dep := range goListDeps(t) {
		for _, forbidden := range operatorPackages {
			if dep == forbidden || strings.HasPrefix(dep, forbidden+"/") {
				t.Errorf("internal/cli/buildinfo reaches %q (via the closure of its imports); "+
					"the CLI is a client of the frozen schema, not of the operator's runtime (D20)", dep)
			}
		}
	}
}

// selfPackage is this package, which `go list -deps` includes in its own closure.
const selfPackage = "github.com/kuberecord/kuberecord/internal/cli/buildinfo"

// TestOnlyTheStandardLibraryIsReached is this package's version of the
// non-vacuity half its siblings get from reaching the read-plane contract.
//
// It cannot make that assertion: this package deliberately depends on nothing,
// so a check that it reaches internal/query would fail by design. What it
// asserts instead is the stronger property, and the one the package exists for.
// `kuberecord version` is what somebody runs when something else has already
// gone wrong — a backend they cannot reach, an archive they cannot read, a
// binary they are not sure of — and a version command that could fail to
// initialise because of a dependency it did not need would be missing at exactly
// the moment it is worth having.
//
// The test is not vacuous in the way its siblings would be, because it fails on
// the first import of anything at all: a domain-shaped first path element is
// what distinguishes a module from a standard-library package.
func TestOnlyTheStandardLibraryIsReached(t *testing.T) {
	deps := goListDeps(t)
	if len(deps) == 0 {
		t.Fatal("go list -deps reported no dependencies at all, not even the standard " +
			"library; the closure is not being read")
	}
	for _, dep := range deps {
		if dep == selfPackage {
			// `go list -deps` includes the package it was asked about.
			continue
		}
		first, _, _ := strings.Cut(dep, "/")
		if strings.Contains(first, ".") {
			t.Errorf("internal/cli/buildinfo reaches %q, which is outside the standard library; "+
				"this package is stamped by the linker and read by `version`, and it must not be "+
				"able to fail for a reason of somebody else's (Task 12.1)", dep)
		}
	}
}

// goListDeps returns the full transitive dependency closure of the package in
// the working directory.
//
// Shelling out rather than parsing the source is what the transitive check needs
// — only the go command knows the closure — and it is accurate about build
// constraints in a way an import-block parse is not.
func goListDeps(t *testing.T) []string {
	t.Helper()

	goBin, err := exec.LookPath("go")
	if err != nil {
		// A test binary running without the go command on PATH is possible and
		// is not this package's failure.
		t.Skipf("the go command is not on PATH, so the import boundary cannot be checked: %v", err)
	}

	cmd := exec.Command(goBin, "list", "-deps", ".")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps .: %v\n%s", err, stderr.String())
	}

	var deps []string
	for line := range strings.SplitSeq(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			deps = append(deps, line)
		}
	}
	return deps
}
