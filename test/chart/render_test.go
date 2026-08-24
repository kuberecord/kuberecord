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

// Package chart holds the Helm chart's template tests (Task 2.4).
//
// They render deploy/charts/kuberecord with `helm template` and assert on the
// objects that come out — which values add or remove which object, and above all
// that the chart and `kustomize build config/default` install *the same
// operator*. That last claim is what lets the Phase 1 acceptance suite run
// against either install path with no assertion changed, so it is asserted here
// rather than left to a reviewer's eye.
//
// These are ordinary unit tests: no cluster, no envtest, no Ginkgo. They need
// only the two binaries `make helm` and `make kustomize` put in bin/, which the
// `test` target depends on.
package chart

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

const (
	// chartRelease is the release name every test renders with. It is the name the
	// documented install uses, and it is load-bearing: because it matches the
	// chart name, the fullname helper collapses to plain "kuberecord" and every
	// object comes out named exactly as the kustomize install names it.
	chartRelease = "kuberecord"
	// chartNamespace is where the operator is installed, matching config/default.
	chartNamespace = "kuberecord-system"
	// renderKubeVersion is the Kubernetes version handed to `helm template`.
	// Without a cluster to ask, Helm assumes a very old version and refuses the
	// chart's own `kubeVersion` floor, so a value has to be supplied; any release
	// at or above that floor renders identically, so this is not a pin of
	// anything — the pinned version that manifests are *validated* against lives
	// in the Makefile (KUBECONFORM_K8S_VERSION).
	renderKubeVersion = "1.35.0"

	// The paths the chart's generated copies are synced from.
	chartDir      = "deploy/charts/kuberecord"
	configCRDDir  = "config/crd/bases"
	configPresets = "config/rbac/presets"
)

// Kinds the tests reach for by name often enough to be worth a constant.
const (
	kindClusterRole        = "ClusterRole"
	kindClusterRoleBinding = "ClusterRoleBinding"
	kindRole               = "Role"
	kindRoleBinding        = "RoleBinding"
	kindDeployment         = "Deployment"
	kindService            = "Service"
	kindServiceAccount     = "ServiceAccount"
	kindClickHouseSink     = "ClickHouseSink"
	kindS3Sink             = "S3Sink"
	kindCRD                = "CustomResourceDefinition"
)

// object is one rendered manifest: its identity, its raw YAML (so a test can
// decode it into whatever typed struct it wants to assert on) and its generic
// decoding (so a test can reach a single field without inventing a struct).
type object struct {
	kind      string
	name      string
	namespace string
	raw       []byte
	fields    map[string]any
}

// decode unmarshals the object into out, failing the test if it cannot.
func (o object) decode(t *testing.T, out any) {
	t.Helper()
	if err := yaml.Unmarshal(o.raw, out); err != nil {
		t.Fatalf("decoding %s: %v", o, err)
	}
}

func (o object) String() string { return o.kind + "/" + o.name }

// manifest is a rendered set of objects keyed by "Kind/name".
//
// Kind is part of the key because two objects legitimately share a name here:
// the manager's permissions are a ClusterRole *and* a namespaced Role, both
// called `<release>-manager-role`, and conflating them would hide the boundary
// that split is there to draw (D7).
type manifest map[string]object

func (m manifest) get(t *testing.T, kind, name string) object {
	t.Helper()
	obj, ok := m[kind+"/"+name]
	if !ok {
		t.Fatalf("expected %s/%s in the rendered output; got %v", kind, name, m.keys())
	}
	return obj
}

func (m manifest) has(kind, name string) bool {
	_, ok := m[kind+"/"+name]
	return ok
}

// keys returns every object's key, sorted, for failure messages.
func (m manifest) keys() []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sortStrings(keys)
	return keys
}

// sortStrings sorts in place so failure messages and comparisons are stable
// rather than in Go's map order.
func sortStrings(in []string) { slices.Sort(in) }

// repoRoot walks up from the test's working directory to the directory holding
// go.mod. Every path in these tests is resolved from there, so they do not care
// where `go test` was invoked from.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod found above %s", dir)
		}
		dir = parent
	}
}

// tool resolves one of the repo's bin/ tools: an explicit override wins (that is
// how CI or a developer points at a system install), then the bin/ copy `make
// helm` / `make kustomize` produce, then PATH.
//
// A missing tool skips rather than fails. `make test` depends on both targets, so
// in the workflow that matters they are always there; skipping keeps a bare
// `go test ./...` on a fresh checkout from failing for a reason that has nothing
// to do with the code.
func tool(t *testing.T, envVar, name string) string {
	t.Helper()
	if override := os.Getenv(envVar); override != "" {
		return override
	}
	local := filepath.Join(repoRoot(t), "bin", name)
	if _, err := os.Stat(local); err == nil {
		return local
	}
	found, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s not found in bin/ or PATH (run `make %s`); skipping", name, name)
	}
	return found
}

// renderArgs is one rendering's inputs: values files (relative to the chart) and
// --set overrides.
type renderArgs struct {
	valuesFiles []string
	sets        []string
}

func (r renderArgs) flags() []string {
	flags := make([]string, 0, 2*(len(r.valuesFiles)+len(r.sets)))
	for _, file := range r.valuesFiles {
		flags = append(flags, "--values", file)
	}
	for _, set := range r.sets {
		flags = append(flags, "--set", set)
	}
	return flags
}

// render runs `helm template` and parses the result. --include-crds is always
// passed, because the CRDs are part of what `helm install` applies and therefore
// part of what these tests are entitled to assert on.
func render(t *testing.T, args renderArgs) manifest {
	t.Helper()
	out, err := renderRaw(t, args)
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	return parseManifest(t, out)
}

// renderRaw returns helm's output and error unjudged, for the tests that assert a
// value combination is *rejected*.
func renderRaw(t *testing.T, args renderArgs) (string, error) {
	t.Helper()
	root := repoRoot(t)
	helm := tool(t, "HELM", "helm")
	cmd := exec.Command(helm, append([]string{
		"template", chartRelease, chartDir,
		"--namespace", chartNamespace,
		"--kube-version", renderKubeVersion,
		"--include-crds",
	}, args.flags()...)...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// kustomizeDefault builds config/default: the other install path, and the
// reference the parity tests compare the chart against.
func kustomizeDefault(t *testing.T) manifest {
	t.Helper()
	root := repoRoot(t)
	kustomize := tool(t, "KUSTOMIZE", "kustomize")
	cmd := exec.Command(kustomize, "build", "config/default")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kustomize build config/default failed: %v\n%s", err, out)
	}
	return parseManifest(t, string(out))
}

// parseManifest splits a multi-document YAML stream into objects. Documents with
// no kind (Helm emits an empty one wherever a template is fully disabled) are
// dropped rather than keyed as ""/"" — their absence is the assertion, not their
// presence.
func parseManifest(t *testing.T, in string) manifest {
	t.Helper()
	out := manifest{}
	reader := utilyaml.NewYAMLReader(bufio.NewReader(strings.NewReader(in)))
	for {
		doc, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("reading rendered YAML: %v", err)
		}
		if len(bytes.TrimSpace(doc)) == 0 {
			continue
		}
		fields := map[string]any{}
		if err := yaml.Unmarshal(doc, &fields); err != nil {
			t.Fatalf("parsing rendered document: %v\n%s", err, doc)
		}
		kind, _ := fields["kind"].(string)
		if kind == "" {
			continue
		}
		name, namespace := metaName(fields)
		key := kind + "/" + name
		if _, clash := out[key]; clash {
			t.Fatalf("two rendered objects share the key %s", key)
		}
		out[key] = object{
			kind:      kind,
			name:      name,
			namespace: namespace,
			raw:       doc,
			fields:    fields,
		}
	}
	return out
}

// metaName pulls name and namespace out of a generically-decoded object.
func metaName(fields map[string]any) (name, namespace string) {
	meta, _ := fields["metadata"].(map[string]any)
	if meta == nil {
		return "", ""
	}
	name, _ = meta["name"].(string)
	namespace, _ = meta["namespace"].(string)
	return name, namespace
}

// labelsOf returns an object's metadata labels.
func labelsOf(t *testing.T, obj object) map[string]string {
	t.Helper()
	var decoded struct {
		Metadata struct {
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
	}
	obj.decode(t, &decoded)
	return decoded.Metadata.Labels
}

// readFile reads a repo-relative file.
func readFile(t *testing.T, relPath string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), relPath))
	if err != nil {
		t.Fatalf("reading %s: %v", relPath, err)
	}
	return data
}

// listYAML lists the sorted base names of the YAML files in a repo-relative
// directory.
func listYAML(t *testing.T, relDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(repoRoot(t), relDir))
	if err != nil {
		t.Fatalf("reading %s: %v", relDir, err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".yaml") {
			names = append(names, entry.Name())
		}
	}
	sortStrings(names)
	return names
}

// containerArgs returns the manager container's arguments.
func containerArgs(t *testing.T, deployment object) []string {
	t.Helper()
	return managerContainer(t, deployment).Args
}

// container is the subset of a container's spec these tests assert on.
type container struct {
	Name            string   `json:"name"`
	Image           string   `json:"image"`
	ImagePullPolicy string   `json:"imagePullPolicy"`
	Command         []string `json:"command"`
	Args            []string `json:"args"`
	Env             []struct {
		Name      string `json:"name"`
		Value     string `json:"value"`
		ValueFrom *struct {
			FieldRef *struct {
				FieldPath string `json:"fieldPath"`
			} `json:"fieldRef"`
		} `json:"valueFrom"`
	} `json:"env"`
	Ports []struct {
		Name          string `json:"name"`
		ContainerPort int    `json:"containerPort"`
	} `json:"ports"`
	Resources struct {
		Limits   map[string]string `json:"limits"`
		Requests map[string]string `json:"requests"`
	} `json:"resources"`
	SecurityContext map[string]any `json:"securityContext"`
	LivenessProbe   *probe         `json:"livenessProbe"`
	ReadinessProbe  *probe         `json:"readinessProbe"`
}

type probe struct {
	HTTPGet struct {
		Path string `json:"path"`
		Port int    `json:"port"`
	} `json:"httpGet"`
}

// deploymentSpec is the subset of the manager Deployment these tests assert on.
type deploymentSpec struct {
	Spec struct {
		Replicas int `json:"replicas"`
		Selector struct {
			MatchLabels map[string]string `json:"matchLabels"`
		} `json:"selector"`
		Template struct {
			Metadata struct {
				Labels      map[string]string `json:"labels"`
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
			Spec struct {
				ServiceAccountName            string         `json:"serviceAccountName"`
				SecurityContext               map[string]any `json:"securityContext"`
				TerminationGracePeriodSeconds int            `json:"terminationGracePeriodSeconds"`
				Containers                    []container    `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
}

// managerContainer decodes a Deployment and returns its sole container, which is
// the manager: the operator runs no sidecars, and a chart that added one would
// have to say so here.
func managerContainer(t *testing.T, deployment object) container {
	t.Helper()
	var decoded deploymentSpec
	deployment.decode(t, &decoded)
	containers := decoded.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("expected exactly one container in %s, got %d", deployment, len(containers))
	}
	return containers[0]
}

// assertYAMLEqual compares two values by their YAML rendering. Comparing the
// serialized form rather than the Go values keeps failure output readable — the
// thing being compared is a manifest fragment, so that is how it is reported —
// and sidesteps pointer identity in decoded structs.
func assertYAMLEqual(t *testing.T, subject string, want, got any) {
	t.Helper()
	wantYAML, err := yaml.Marshal(want)
	if err != nil {
		t.Fatalf("marshalling expected %s: %v", subject, err)
	}
	gotYAML, err := yaml.Marshal(got)
	if err != nil {
		t.Fatalf("marshalling actual %s: %v", subject, err)
	}
	if !bytes.Equal(wantYAML, gotYAML) {
		t.Errorf("%s differs:\n got:\n%s\nwant:\n%s", subject, gotYAML, wantYAML)
	}
}

// assertEnvEqual compares two containers' environments as name/value pairs,
// including the downward-API reference POD_NAMESPACE is taken from — the operator
// refuses to start without it, and a chart that hard-coded it would move a
// security boundary (D7).
func assertEnvEqual(t *testing.T, want, got container) {
	t.Helper()
	if len(want.Env) != len(got.Env) {
		assertYAMLEqual(t, "container env", want.Env, got.Env)
		return
	}
	for i := range want.Env {
		assertYAMLEqual(t, fmt.Sprintf("container env %q", want.Env[i].Name), want.Env[i], got.Env[i])
	}
}

// hasArg reports whether args contains an exact argument, or any argument with
// the given `--flag=` prefix when prefix is true.
func hasArg(args []string, want string, prefix bool) bool {
	for _, arg := range args {
		if arg == want || (prefix && strings.HasPrefix(arg, want)) {
			return true
		}
	}
	return false
}

// presetNames returns the watch presets the repo ships, derived from
// config/rbac/presets/ rather than listed here — a preset added there without a
// values entry then fails TestPresetValuesCoverEveryShippedPreset instead of
// being silently unavailable to Helm users.
func presetNames(t *testing.T) []string {
	t.Helper()
	files := listYAML(t, configPresets)
	names := make([]string, 0, len(files))
	for _, file := range files {
		names = append(names, strings.TrimSuffix(file, ".yaml"))
	}
	return names
}

// presetRoleName is the ClusterRole a preset renders as, under the conventional
// release name.
func presetRoleName(preset string) string {
	return fmt.Sprintf("%s-watcher-%s", chartRelease, preset)
}
