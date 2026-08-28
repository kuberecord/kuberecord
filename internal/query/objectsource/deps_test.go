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

package objectsource_test

import (
	"os/exec"
	"strings"
	"testing"
)

// The two boundaries this package sits on, and what each one is protecting.
//
// The SDK is confined by the object-store-client-is-confined depguard rule, and this
// test is not a duplicate of it. depguard reads import blocks, so it sees a *direct*
// import and nothing else; the way a confinement is actually lost is through a
// helper package added later that itself links the SDK, which every import-block
// check in the world passes. The whole closure is the property worth stating.
//
// The operator's runtime is denied for the reason the read plane exists separately
// at all: it is a client of the frozen schema, not of the write path's internals.
// internal/sink is on the list even though this package is about object storage,
// because the sink has a read half of its own and the temptation to reach for it is
// real — and taking it would put read-plane concerns on the dependency graph of the
// hot path.
var (
	objectStoreClients = []string{
		"github.com/aws/aws-sdk-go-v2",
		"github.com/aws/aws-sdk-go",
		"github.com/aws/smithy-go",
	}

	operatorPackages = []string{
		"github.com/kuberecord/kuberecord/internal/sink",
		"github.com/kuberecord/kuberecord/internal/pipeline",
		"github.com/kuberecord/kuberecord/internal/watch",
		"github.com/kuberecord/kuberecord/internal/controller",
	}
)

// TestTheSeamLinksNoObjectStoreClient is the reason this package can be the one both
// backends are written against: it speaks the interface and nothing else, so a source
// over a directory needs no credential, no network and no SDK.
func TestTheSeamLinksNoObjectStoreClient(t *testing.T) {
	for _, dep := range deps(t) {
		for _, client := range objectStoreClients {
			if dep == client || strings.HasPrefix(dep, client+"/") {
				t.Errorf("internal/query/objectsource reaches %q through the closure of its "+
					"imports; the object-store client belongs to "+
					"internal/query/objectsource/awssource, and this package speaks the "+
					"ObjectSource interface only", dep)
			}
		}
	}
}

// TestNoOperatorPackagesInTransitiveDeps keeps the read plane a client of the frozen
// schema rather than of the operator's runtime.
func TestNoOperatorPackagesInTransitiveDeps(t *testing.T) {
	for _, dep := range deps(t) {
		for _, forbidden := range operatorPackages {
			if dep == forbidden || strings.HasPrefix(dep, forbidden+"/") {
				t.Errorf("internal/query/objectsource reaches %q through the closure of its "+
					"imports; the read plane must not depend on the operator's runtime", dep)
			}
		}
	}
}

// deps returns this package's whole dependency closure.
//
// Shelling out to the go command rather than parsing import blocks is what makes the
// test transitive — only the go command knows the closure — and it is accurate about
// build constraints in a way a source scan is not.
func deps(t *testing.T) []string {
	t.Helper()

	goBin, err := exec.LookPath("go")
	if err != nil {
		// A test binary running without the go command on PATH is possible and is not
		// this package's failure.
		t.Skipf("the go command is not on PATH, so the dependency closure cannot be checked: %v", err)
	}

	cmd := exec.Command(goBin, "list", "-deps", ".")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps .: %v\n%s", err, stderr.String())
	}

	var lines []string
	for line := range strings.SplitSeq(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
