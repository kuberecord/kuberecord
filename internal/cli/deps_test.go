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
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// operatorPackages are the packages the CLI must never be able to reach (D20).
//
// The first three are the operator's runtime. Coupling to them would make every
// refactor of the write path a release of the CLI, and would drag
// controller-runtime into the link graph of a binary whose only contact with an
// API server is discovery and kubeconfig.
//
// internal/sink is on the list for a reason of its own. It is not merely the
// write path: it has a read half, and a command able to see it would be tempted
// to answer an analyst's question through the operator's warm-up reader — a
// different contract, with opposite pressures, sitting on the hot path's
// dependency graph (D16). The read plane in internal/query is the contract this
// package is a client of, and it is deliberately absent from this list.
var operatorPackages = []string{
	"github.com/kuberecord/kuberecord/internal/pipeline",
	"github.com/kuberecord/kuberecord/internal/watch",
	"github.com/kuberecord/kuberecord/internal/controller",
	"github.com/kuberecord/kuberecord/internal/sink",
}

// readPlaneContract is what the CLI is allowed — and expected — to depend on.
const readPlaneContract = "github.com/kuberecord/kuberecord/internal/query"

// TestNoOperatorPackagesInTransitiveDeps asserts the boundary over the whole
// dependency closure, not just this package's own import block.
//
// Transitive is the interesting part. A direct import is obvious in review; what
// this catches is the reach that arrives through an innocent-looking helper
// package added later, which is how an import boundary is actually lost. It is
// the second of two enforcements — the depguard rule
// `cli-is-a-client-of-the-schema` is the first — because a single enforcement is
// a convention.
func TestNoOperatorPackagesInTransitiveDeps(t *testing.T) {
	for _, dep := range goListDeps(t) {
		for _, forbidden := range operatorPackages {
			if dep == forbidden || strings.HasPrefix(dep, forbidden+"/") {
				t.Errorf("internal/cli reaches %q (via the closure of its imports); "+
					"the CLI is a client of the frozen schema, not of the operator's runtime (D20)", dep)
			}
		}
	}
}

// TestTheReadPlaneContractIsActuallyReached is the non-vacuity half.
//
// A closure check passes trivially against a package that imports nothing, and a
// boundary test that would keep passing while the thing it guards was deleted
// certifies nothing. This asserts the CLI really is wired to the contract it is
// supposed to be a client of, so that the test above is measuring a boundary that
// something is actually pressing against.
func TestTheReadPlaneContractIsActuallyReached(t *testing.T) {
	if !slices.Contains(goListDeps(t), readPlaneContract) {
		t.Errorf("internal/cli no longer reaches %q, so TestNoOperatorPackagesInTransitiveDeps "+
			"is passing over an empty closure rather than over a boundary", readPlaneContract)
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
