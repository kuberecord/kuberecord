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

// Package utils holds the process-level plumbing the e2e suite needs before it
// has a cluster to talk to: running commands from the project root and getting
// images into the kind node.
//
// It deliberately stops there. Anything that speaks Kubernetes or ClickHouse
// lives in the suite itself (test/e2e), where it can use Ginkgo's failure
// handling directly instead of threading errors back through a helper package.
package utils

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2" // nolint:revive,staticcheck
)

const (
	defaultKindBinary  = "kind"
	defaultKindCluster = "kind"
)

// Run executes the provided command within this context
func Run(cmd *exec.Cmd) (string, error) {
	dir, _ := GetProjectDir()
	cmd.Dir = dir

	if err := os.Chdir(cmd.Dir); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "chdir dir: %q\n", err)
	}

	cmd.Env = append(os.Environ(), "GO111MODULE=on")
	command := strings.Join(cmd.Args, " ")
	_, _ = fmt.Fprintf(GinkgoWriter, "running: %q\n", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%q failed with error %q: %w", command, string(output), err)
	}

	return string(output), nil
}

// LoadImageToKindClusterWithName loads a local docker image to the kind cluster
func LoadImageToKindClusterWithName(name string) error {
	return runKind("load", "docker-image", name)
}

// LoadImageArchiveToKindCluster loads a `docker save` archive into the kind
// cluster.
//
// It exists for images pulled from a registry as a multi-platform index, which
// `kind load docker-image` cannot handle: kind imports with --all-platforms, so
// containerd goes looking for the manifests of every platform in the index and
// fails on the ones a single-platform `docker pull` never fetched. Exporting one
// platform to an archive first sidesteps the index entirely.
func LoadImageArchiveToKindCluster(archivePath string) error {
	return runKind("load", "image-archive", archivePath)
}

// runKind invokes the kind binary against the cluster under test. Both are
// overridable by environment variable (KIND, KIND_CLUSTER), which is how the
// Makefile keeps the suite and its own cluster-creation target in agreement.
func runKind(args ...string) error {
	cluster := defaultKindCluster
	if v, ok := os.LookupEnv("KIND_CLUSTER"); ok {
		cluster = v
	}
	kindBinary := defaultKindBinary
	if v, ok := os.LookupEnv("KIND"); ok {
		kindBinary = v
	}
	_, err := Run(exec.Command(kindBinary, append(args, "--name", cluster)...))
	return err
}

// GetProjectDir will return the directory where the project is
func GetProjectDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return wd, fmt.Errorf("failed to get current working directory: %w", err)
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	return wd, nil
}
