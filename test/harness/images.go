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

package harness

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/yelzhy/kuberecord/test/utils"
)

// SideloadImage puts a published image into the kind node, pulling it to the
// host first only if it is not already there.
//
// The alternative — letting the kubelet pull it — would put a several-hundred-
// megabyte registry download inside a suite's runtime budget on every cold run,
// and would make the suite fail on a machine with no registry access even though
// nothing about it needs one. For the chaos suite it is stronger than that: a
// pull that happens *during* an outage window would be counted against the
// writer's retry budget, so a recovery assertion could fail for a reason that has
// nothing to do with the operator.
//
// It goes through a `docker save` archive rather than `kind load docker-image`
// because published images are usually multi-platform indexes, which kind cannot
// handle: it imports with --all-platforms, and containerd then fails looking for
// the manifests of the platforms a single-platform pull never fetched. Exporting
// this host's platform alone produces a plain single-platform archive kind
// imports without complaint. (`docker save --platform` needs Docker 25 or newer.)
func SideloadImage(image string) {
	GinkgoHelper()
	if _, err := utils.Run(exec.Command("docker", "image", "inspect", image)); err != nil {
		By("pulling " + image + " to the host")
		out, pullErr := utils.Run(exec.Command("docker", "pull", image))
		Expect(pullErr).NotTo(HaveOccurred(), "Failed to pull %s: %s", image, out)
	}

	archiveDir, err := os.MkdirTemp("", "kubestream-images")
	Expect(err).NotTo(HaveOccurred(), "Failed to create a temporary directory for the image archive")
	DeferCleanup(func() {
		if err := os.RemoveAll(archiveDir); err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "cleanup: removing the image archive: %v\n", err)
		}
	})

	// The kind node runs on the host's Docker, so the host's architecture is the
	// node's architecture; the test binary is built for it too.
	archive := filepath.Join(archiveDir, "image.tar")
	out, err := utils.Run(exec.Command("docker", "save",
		"--platform", "linux/"+runtime.GOARCH, image, "-o", archive))
	Expect(err).NotTo(HaveOccurred(), "Failed to export %s: %s", image, out)

	Expect(utils.LoadImageArchiveToKindCluster(archive)).To(Succeed(),
		"Failed to load %s into Kind", image)
}
