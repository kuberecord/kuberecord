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

// Package harness is the cluster-side vocabulary kuberecord's acceptance suites
// are written in: thin kubectl wrappers, ClickHouse querying, status-condition
// and pod inspection, and the manifests a scenario applies.
//
// It exists because two suites make the same claims about the same system from
// different angles — test/e2e proves the happy paths of Phase 1, test/chaos
// proves the failure paths of Phase 2 — and Task 2.1 requires the chaos suite's
// restart scenario to *reuse* the e2e restart assertions rather than paraphrase
// them. Sharing the query layer is what makes that literal: both suites read the
// sink through one definition of "the rows for this object", so a schema change
// breaks them together instead of silently leaving one asserting on a column that
// no longer means what it used to.
//
// It stops at the vocabulary. Timeouts, fixture names and the assertions
// themselves stay in each suite, because those are exactly the things the two
// suites legitimately disagree about — a chaos scenario waits out a three-minute
// outage where an e2e scenario would call the same wait a hang.
//
// Helpers here fail through Gomega (rather than returning errors) wherever the
// failure can only mean "the cluster is not in the state this scenario needs",
// so a caller reads as a scenario rather than as error plumbing. Anything a
// caller might legitimately want to poll on — a query, a UID, a condition —
// returns an error instead, so it can be retried inside an Eventually.
package harness

import (
	"encoding/base64"
	"fmt"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/kuberecord/kuberecord/test/utils"
)

// Kubectl runs one kubectl invocation from the project root and returns its
// combined output.
func Kubectl(args ...string) (string, error) {
	return utils.Run(exec.Command("kubectl", args...))
}

// KubectlStdin runs kubectl with manifest piped to its stdin.
//
// Manifests that vary per scenario (a namespace, an object name, a replica
// count) are applied this way rather than templated onto disk: there is no
// temporary file to clean up, the YAML is readable next to the assertion that
// depends on it, and a scenario cannot accidentally inherit another's leftover
// file.
func KubectlStdin(manifest string, args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(manifest)
	return utils.Run(cmd)
}

// ApplyYAML server-side-applies a manifest under the given field manager.
//
// Server-side apply (rather than the client-side default) is what puts the
// manager's name into the object's managedFields, and therefore into the actors
// column the suites assert on. A plain `kubectl create` would attribute the
// object to whatever manager name that kubectl build happens to use, making
// "the applying manager appears in actors" a substring guess rather than a
// precise claim.
func ApplyYAML(fieldManager, manifest string) {
	GinkgoHelper()
	out, err := KubectlStdin(manifest, "apply", "--server-side",
		"--field-manager="+fieldManager, "-f", "-")
	Expect(err).NotTo(HaveOccurred(), "failed to apply manifest: %s", out)
}

// ClientSideApplyYAML applies a manifest the old way — client-side, without
// --server-side.
//
// It exists for one scenario and should not be the default: client-side apply is
// what writes `kubectl.kubernetes.io/last-applied-configuration`, stuffing a
// verbatim copy of the whole submitted object into an annotation on it. Every
// other suite wants server-side apply (see ApplyYAML) for the actors column; the
// redaction scenario wants precisely this annotation, because scrubbing it is the
// always-on half of Task 3.3 and a fixture that never produced one would assert
// nothing.
func ClientSideApplyYAML(manifest string) {
	GinkgoHelper()
	out, err := KubectlStdin(manifest, "apply", "-f", "-")
	Expect(err).NotTo(HaveOccurred(), "failed to apply manifest: %s", out)
}

// ApplyFile applies a manifest that lives in the repository. The path is
// relative to the project root, which is the directory every command here runs
// in.
func ApplyFile(path string) {
	GinkgoHelper()
	out, err := Kubectl("apply", "-f", path)
	Expect(err).NotTo(HaveOccurred(), "failed to apply %s: %s", path, out)
}

// ApplyFileAs server-side-applies a manifest that lives in the repository, under
// the given field manager.
//
// It is ApplyFile plus the two flags ApplyYAML explains: server-side apply is
// what puts the manager's name into the object's managedFields, and therefore
// into the actors column the suites assert on. ApplyFile stays the way in for
// fixtures nothing asserts authorship of.
func ApplyFileAs(fieldManager, path string) {
	GinkgoHelper()
	out, err := Kubectl("apply", "--server-side", "--field-manager="+fieldManager, "-f", path)
	Expect(err).NotTo(HaveOccurred(), "failed to apply %s: %s", path, out)
}

// ApplyKustomization applies a kustomization directory, by repository-relative
// path.
//
// It exists so a suite can apply a *published example* rather than a copy of one:
// the tee scenario's overlay (test/e2e/manifests/tee) is examples/tee with one
// address patched, and rendering it through kubectl's own kustomize is what keeps
// the thing CI applies and the thing a reader copies the same file. A plain
// apply, not server-side: nothing asserts authorship of a sink or a rule, and the
// objects that are asserted on go through ApplyFileAs or ApplyYAML.
func ApplyKustomization(dir string) {
	GinkgoHelper()
	out, err := Kubectl("apply", "-k", dir)
	Expect(err).NotTo(HaveOccurred(), "failed to apply the kustomization at %s: %s", dir, out)
}

// SecretValue reads and base64-decodes one key of a Secret.
//
// A suite needs this where a fixture's credentials are defined by the manifest it
// applied rather than by the suite itself: reading them back out of the cluster
// is what makes "the harness authenticates as the identity this manifest
// created" true by construction, instead of by two constants somebody has to keep
// in step.
func SecretValue(name, namespace, key string) (string, error) {
	out, err := Kubectl("get", "secret", name, "-n", namespace, "-o", "jsonpath={.data."+key+"}")
	if err != nil {
		return "", err
	}
	encoded := strings.TrimSpace(out)
	if encoded == "" {
		return "", fmt.Errorf("secret %s/%s carries no %q key", namespace, name, key)
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("decode %s/%s key %q: %w", namespace, name, key, err)
	}
	return string(decoded), nil
}

// DeleteResource removes one object and waits for it to be gone. Failures are
// reported, not swallowed: a delete that silently did nothing would leave the
// next scenario running against state it did not create (Invariant 4's spirit,
// applied to the suite itself).
func DeleteResource(kind, name, namespace string) {
	GinkgoHelper()
	out, err := Kubectl(deleteArgs(kind, name, namespace)...)
	Expect(err).NotTo(HaveOccurred(), "failed to delete %s/%s: %s", kind, name, out)
}

// DeleteResourceQuietly is DeleteResource for cleanup paths that must not fail a
// spec — an AfterEach tidying up after an assertion has already failed, where a
// second failure would only bury the first.
func DeleteResourceQuietly(kind, name, namespace string) {
	if out, err := Kubectl(deleteArgs(kind, name, namespace)...); err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "cleanup: deleting %s/%s: %v\n%s", kind, name, err, out)
	}
}

func deleteArgs(kind, name, namespace string) []string {
	args := []string{"delete", kind, name, "--ignore-not-found", "--wait"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	return args
}

// CreateNamespace makes a namespace exist and be empty, deleting any previous
// incarnation first.
//
// The delete is what makes a suite recoverable. Scenarios assert exact row
// counts, so each needs to start from a namespace with nothing in it — but a
// suite also reuses whatever Kind cluster it finds, so refusing to run when the
// namespace already exists would let one interrupted run poison every run after
// it. Recreating gives the isolation the assertions need and repairs a dirty
// cluster on the way, instead of demanding a clean one.
//
// Within a run this can never mask a collision: every scenario uses its own
// namespace, so the only thing that could be here is debris from a previous run,
// which is exactly what should be cleared.
func CreateNamespace(name string) {
	GinkgoHelper()
	out, err := Kubectl("delete", "namespace", name, "--ignore-not-found", "--wait")
	Expect(err).NotTo(HaveOccurred(), "failed to clear namespace %s: %s", name, out)
	out, err = Kubectl("create", "namespace", name)
	Expect(err).NotTo(HaveOccurred(), "failed to create namespace %s: %s", name, out)
}

// ObjectUID reads an object's UID, which is the identity sink rows are keyed by
// in every reincarnation and offline-deletion assertion.
func ObjectUID(kind, name, namespace string) (string, error) {
	args := []string{"get", kind, name, "-o", "jsonpath={.metadata.uid}"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	out, err := Kubectl(args...)
	if err != nil {
		return "", err
	}
	uid := strings.TrimSpace(out)
	if uid == "" {
		return "", fmt.Errorf("%s/%s has no UID yet", kind, name)
	}
	return uid, nil
}

// ObjectJSON returns one object exactly as the API server stores it. It is the
// input a suite recomputes an object's canonical hash from (see
// pipeline.ObjectHash), so it must be the raw object rather than a projection.
func ObjectJSON(kind, name, namespace string) ([]byte, error) {
	args := []string{"get", kind, name, "-o", "json"}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	out, err := Kubectl(args...)
	if err != nil {
		return nil, err
	}
	// utils.Run merges stderr in, and kubectl may prepend its own notices; the
	// document itself starts at the first brace.
	start := strings.Index(out, "{")
	if start < 0 {
		return nil, fmt.Errorf("no JSON document in the output for %s/%s: %s", kind, name, out)
	}
	return []byte(out[start:]), nil
}
