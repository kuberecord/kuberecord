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

package controller

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// Paths to the manifests this package's +kubebuilder:rbac markers generate into,
// and to the hand-written aggregation manifests that surround them.
const (
	rbacDir     = "../../config/rbac"
	presetsDir  = "../../config/rbac/presets"
	docsRBAC    = "../../docs/RBAC.md"
	readmePath  = "../../README.md"
	watcherFile = "watcher_role.yaml"
)

// aggregateLabel is the label whose presence on a ClusterRole makes the
// controller-manager fold that role's rules into kubestream-watcher.
const aggregateLabel = "kuberecord.io/aggregate-to-watcher"

// The RBAC kind names, spelled once each: the difference between `Role` and
// `ClusterRole` is the entire point of several assertions below, so a typo in one
// of them would quietly turn a scope check into a tautology.
const (
	kindRole               = "Role"
	kindClusterRole        = "ClusterRole"
	kindRoleBinding        = "RoleBinding"
	kindClusterRoleBinding = "ClusterRoleBinding"
)

// These tests assert the *shipped RBAC manifests*, not behavior, and they live in
// this package because this is where the markers that generate config/rbac/role.yaml
// live (mirroring api/v1alpha1/crdmanifests_test.go, which guards the CRD markers
// from the package that owns them).
//
// Why assert YAML at all: every security property of the RBAC model is a property
// of a manifest, and the failure mode being defended against is a quiet widening
// — someone adds `secrets` to a preset, or promotes the Secret Role to a
// ClusterRole, and nothing anywhere fails. A cluster would accept all of it
// happily. These tests are the only thing that would not.

// rbacObject is the subset of every RBAC kind these tests need. The manifests
// hold four different kinds; decoding them all into one shape avoids a
// type-switch per file and keeps the assertions readable.
type rbacObject struct {
	Kind     string `json:"kind"`
	Metadata struct {
		Name      string            `json:"name"`
		Namespace string            `json:"namespace"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
	AggregationRule *rbacv1.AggregationRule `json:"aggregationRule"`
	Rules           []rbacv1.PolicyRule     `json:"rules"`
	RoleRef         rbacv1.RoleRef          `json:"roleRef"`
	Subjects        []rbacv1.Subject        `json:"subjects"`
}

// loadRBACDocs decodes every YAML document in one manifest file. role.yaml is a
// multi-document file (the ClusterRole and the namespaced Role controller-gen
// emits from the same marker set), so splitting is mandatory rather than
// defensive.
func loadRBACDocs(t *testing.T, path string) []rbacObject {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	var objs []rbacObject
	for doc := range strings.SplitSeq(string(raw), "\n---") {
		if strings.TrimSpace(stripYAMLComments(doc)) == "" {
			continue
		}
		var obj rbacObject
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("parsing %s: %v", path, err)
		}
		if obj.Kind == "" {
			t.Fatalf("a document in %s has no kind", path)
		}
		objs = append(objs, obj)
	}
	if len(objs) == 0 {
		t.Fatalf("%s contains no RBAC objects", path)
	}
	return objs
}

// stripYAMLComments removes whole-line comments so that a file whose leading
// document is nothing but the rationale header is not mistaken for an object.
func stripYAMLComments(doc string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(doc, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	return b.String()
}

// allRBACFiles lists every manifest under config/rbac, presets included. Tests
// that assert a *global* absence (no ClusterRole anywhere grants secrets) must
// walk the tree rather than a hardcoded list, or a new file becomes a hole.
func allRBACFiles(t *testing.T) []string {
	t.Helper()

	var files []string
	err := filepath.WalkDir(rbacDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Base(path) == "kustomization.yaml" {
			return nil
		}
		if ext := filepath.Ext(path); ext == ".yaml" || ext == ".yml" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", rbacDir, err)
	}
	return files
}

// findObject returns the single object of the given kind and name, failing if it
// is absent — an assertion in its own right, since a renamed or deleted manifest
// should fail loudly rather than vacuously pass.
func findObject(t *testing.T, objs []rbacObject, kind, name string) rbacObject {
	t.Helper()

	for _, obj := range objs {
		if obj.Kind == kind && obj.Metadata.Name == name {
			return obj
		}
	}
	t.Fatalf("no %s named %q among the loaded manifests", kind, name)
	return rbacObject{}
}

// TestBaseClusterRoleGrantsOnlyControlPlaneRights pins the generated base role to
// the control plane's actual needs.
//
// The rule set is asserted exhaustively — every grant must be expected *and*
// every expectation must be present — because the interesting failure is an
// added rule, which a "contains" assertion would never notice. Adding a
// legitimate grant means adding it here too, which is the intended friction:
// widening the operator's standing cluster-wide reach should be a deliberate,
// reviewed act.
func TestBaseClusterRoleGrantsOnlyControlPlaneRights(t *testing.T) {
	objs := loadRBACDocs(t, filepath.Join(rbacDir, "role.yaml"))
	role := findObject(t, objs, kindClusterRole, "manager-role")

	// Keyed as "group/resource" -> sorted verbs.
	want := map[string][]string{
		"/events":     {"create", "patch"},
		"/namespaces": {"get", "list", "watch"},
		"authorization.k8s.io/selfsubjectaccessreviews": {"create"},
		"kuberecord.io/clickhousesinks":                 {"get", "list", "watch"},
		"kuberecord.io/clusterstreamrules":              {"get", "list", "watch"},
		"kuberecord.io/streamrules":                     {"get", "list", "watch"},
		"kuberecord.io/clickhousesinks/status":          {"get", "patch", "update"},
		"kuberecord.io/clusterstreamrules/status":       {"get", "patch", "update"},
		"kuberecord.io/streamrules/status":              {"get", "patch", "update"},
	}

	got := map[string][]string{}
	for _, rule := range role.Rules {
		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				verbs := slices.Clone(rule.Verbs)
				slices.Sort(verbs)
				got[group+"/"+resource] = verbs
			}
		}
	}

	for key, wantVerbs := range want {
		gotVerbs, ok := got[key]
		if !ok {
			t.Errorf("base ClusterRole is missing its grant on %s (run `make manifests`?)", key)
			continue
		}
		if !slices.Equal(gotVerbs, wantVerbs) {
			t.Errorf("base ClusterRole grants %v on %s; want %v", gotVerbs, key, wantVerbs)
		}
	}
	for key := range got {
		if _, expected := want[key]; !expected {
			t.Errorf("base ClusterRole grants %s, which no control-plane reconciler needs; "+
				"watch rights belong in an aggregated preset, not the base role", key)
		}
	}
}

// TestBaseClusterRoleHasNoWorkloadGrants is the standing-reach assertion, kept
// separate from the exhaustive rule check because it encodes a different promise:
// not merely "the rules are the expected ones" but "no workload kind is readable
// without an administrator applying a preset".
//
// The deleted per-GVK reconcilers carried pods/services/deployments; this is what
// stops them coming back by accident.
func TestBaseClusterRoleHasNoWorkloadGrants(t *testing.T) {
	forbidden := []string{
		"pods", "services", "configmaps", "serviceaccounts", "endpoints",
		"deployments", "replicasets", "statefulsets", "daemonsets",
		"jobs", "cronjobs", "ingresses", "networkpolicies",
		"persistentvolumes", "persistentvolumeclaims", "nodes",
	}

	objs := loadRBACDocs(t, filepath.Join(rbacDir, "role.yaml"))
	role := findObject(t, objs, kindClusterRole, "manager-role")

	for _, rule := range role.Rules {
		if slices.Contains(rule.Resources, "*") || slices.Contains(rule.APIGroups, "*") {
			t.Errorf("base ClusterRole uses a wildcard (%v on %v); every grant must be explicit",
				rule.Resources, rule.APIGroups)
		}
		for _, resource := range rule.Resources {
			if slices.Contains(forbidden, resource) {
				t.Errorf("base ClusterRole grants %q; workload reads must arrive via an "+
					"aggregated watch preset so they can be revoked without a redeploy", resource)
			}
		}
	}
}

// TestSecretRightsAreNamespaceScoped asserts the credential boundary that makes
// ClickHouseSink.spec.connection.credentialsSecretRef.namespace's default mean
// something: the operator's Secret grant exists in exactly one place, and that
// place is a namespaced Role.
//
// Both halves matter. If the Role vanished, sinks would silently stop resolving
// credentials; if any ClusterRole gained `secrets`, a cluster-scoped sink could
// aim the operator at any Secret in the cluster.
func TestSecretRightsAreNamespaceScoped(t *testing.T) {
	t.Run("granted by a namespaced Role", func(t *testing.T) {
		objs := loadRBACDocs(t, filepath.Join(rbacDir, "role.yaml"))
		role := findObject(t, objs, kindRole, "manager-role")

		if role.Metadata.Namespace == "" {
			t.Error("the Secret-reading Role declares no namespace; kustomize's namespace " +
				"transformer needs one to rewrite to the deployment namespace")
		}

		var found bool
		for _, rule := range role.Rules {
			if slices.Contains(rule.Resources, "secrets") {
				found = true
				verbs := slices.Clone(rule.Verbs)
				slices.Sort(verbs)
				if want := []string{"get", "list", "watch"}; !slices.Equal(verbs, want) {
					t.Errorf("Secret grant carries verbs %v; want %v (the operator never writes "+
						"a credential Secret)", verbs, want)
				}
			}
		}
		if !found {
			t.Error("no namespaced Secret grant found; every ClickHouseSink would report " +
				"CredentialsResolved=False")
		}
	})

	t.Run("granted by no ClusterRole anywhere", func(t *testing.T) {
		for _, file := range allRBACFiles(t) {
			for _, obj := range loadRBACDocs(t, file) {
				if obj.Kind != kindClusterRole {
					continue
				}
				for _, rule := range obj.Rules {
					if slices.Contains(rule.Resources, "secrets") {
						t.Errorf("%s: ClusterRole %q grants cluster-wide access to secrets; "+
							"credential reads must stay namespaced and v1/Secret is a denied "+
							"watch kind (D8)", file, obj.Metadata.Name)
					}
				}
			}
		}
	})

	t.Run("bound by a RoleBinding in the operator namespace", func(t *testing.T) {
		objs := loadRBACDocs(t, filepath.Join(rbacDir, "manager_secret_role_binding.yaml"))
		binding := findObject(t, objs, kindRoleBinding, "manager-secret-rolebinding")

		if binding.Metadata.Namespace == "" {
			t.Error("the Secret RoleBinding declares no namespace")
		}
		if binding.RoleRef.Kind != kindRole {
			t.Errorf("the Secret binding references a %s; a RoleBinding to a ClusterRole would "+
				"grant the ClusterRole's rules, defeating the namespacing", binding.RoleRef.Kind)
		}
		if binding.RoleRef.Name != "manager-role" {
			t.Errorf("the Secret binding references Role %q; want the generated manager-role",
				binding.RoleRef.Name)
		}
		assertBoundToOperatorSA(t, binding)
	})
}

// assertBoundToOperatorSA checks that a binding's only subject is the operator's
// ServiceAccount. A stray second subject (a Group, say) would hand kubestream's
// rights to something that is not kubestream.
func assertBoundToOperatorSA(t *testing.T, binding rbacObject) {
	t.Helper()

	if len(binding.Subjects) != 1 {
		t.Fatalf("%s %q has %d subjects; want exactly the operator ServiceAccount",
			binding.Kind, binding.Metadata.Name, len(binding.Subjects))
	}
	subject := binding.Subjects[0]
	if subject.Kind != "ServiceAccount" || subject.Name != "controller-manager" {
		t.Errorf("%s %q is bound to %s/%s; want ServiceAccount/controller-manager",
			binding.Kind, binding.Metadata.Name, subject.Kind, subject.Name)
	}
}

// TestWatcherRoleAggregatesByLabel asserts the mechanism the whole model rests on:
// an empty role that the controller-manager fills from labelled presets.
//
// `rules` must be empty in the manifest. A rule written here would be clobbered
// by the controller-manager on the first sync, so a non-empty list is not a
// stronger grant — it is a lie in the repository about what the cluster will hold.
func TestWatcherRoleAggregatesByLabel(t *testing.T) {
	objs := loadRBACDocs(t, filepath.Join(rbacDir, watcherFile))
	role := findObject(t, objs, kindClusterRole, "watcher")

	if len(role.Rules) != 0 {
		t.Errorf("kubestream-watcher declares %d rule(s) of its own; the controller-manager "+
			"overwrites them from the aggregation selector", len(role.Rules))
	}
	if role.AggregationRule == nil || len(role.AggregationRule.ClusterRoleSelectors) != 1 {
		t.Fatalf("kubestream-watcher must carry exactly one clusterRoleSelector; got %+v",
			role.AggregationRule)
	}

	selector := role.AggregationRule.ClusterRoleSelectors[0]
	if got := selector.MatchLabels[aggregateLabel]; got != "true" {
		t.Errorf("aggregation selector matches %s=%q; want \"true\" — presets label themselves "+
			"with this exact pair", aggregateLabel, got)
	}
	if len(selector.MatchLabels) != 1 || len(selector.MatchExpressions) != 0 {
		t.Errorf("aggregation selector has extra terms (%+v); a preset that satisfies the "+
			"documented label but not a hidden term would silently not aggregate", selector)
	}

	binding := findObject(t,
		loadRBACDocs(t, filepath.Join(rbacDir, "watcher_role_binding.yaml")),
		kindClusterRoleBinding, "watcher-rolebinding")
	if binding.RoleRef.Kind != kindClusterRole || binding.RoleRef.Name != "watcher" {
		t.Errorf("the watcher binding references %s/%s; want ClusterRole/watcher",
			binding.RoleRef.Kind, binding.RoleRef.Name)
	}
	assertBoundToOperatorSA(t, binding)
}

// TestPresetsAreReadOnlyAndLabelled walks every shipped preset. A preset is the
// one place an administrator is expected to copy from, so a preset that granted a
// write verb, a wildcard, or `secrets` would propagate that mistake into
// cluster-specific roles nobody reviews.
func TestPresetsAreReadOnlyAndLabelled(t *testing.T) {
	entries, err := os.ReadDir(presetsDir)
	if err != nil {
		t.Fatalf("reading %s: %v", presetsDir, err)
	}

	// Every family named in the task's RBAC model must exist: a missing preset is
	// a documented grant path that does not actually ship.
	wantPresets := []string{"core-workloads", "networking", "batch", "storage", "rbac-read"}
	var found []string

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		family := strings.TrimSuffix(entry.Name(), ".yaml")
		found = append(found, family)

		t.Run(family, func(t *testing.T) {
			objs := loadRBACDocs(t, filepath.Join(presetsDir, entry.Name()))
			if len(objs) != 1 {
				t.Fatalf("preset %s holds %d objects; one preset file, one ClusterRole",
					entry.Name(), len(objs))
			}
			role := objs[0]

			if role.Kind != kindClusterRole {
				t.Errorf("preset is a %s; presets must be ClusterRoles to be aggregatable",
					role.Kind)
			}
			if role.Metadata.Name != "watcher-"+family {
				t.Errorf("preset is named %q; want watcher-%s so the kustomize-prefixed name "+
					"stays predictable", role.Metadata.Name, family)
			}
			if got := role.Metadata.Labels[aggregateLabel]; got != "true" {
				t.Errorf("preset carries %s=%q; without \"true\" it is applied but never "+
					"aggregated, and grants nothing", aggregateLabel, got)
			}
			if role.AggregationRule != nil {
				t.Error("preset declares an aggregationRule; an aggregated role's rules are " +
					"server-owned, so its contents would never reach kubestream-watcher")
			}
			if len(role.Rules) == 0 {
				t.Fatal("preset grants nothing")
			}

			for _, rule := range role.Rules {
				verbs := slices.Clone(rule.Verbs)
				slices.Sort(verbs)
				if want := []string{"get", "list", "watch"}; !slices.Equal(verbs, want) {
					t.Errorf("rule on %v grants verbs %v; presets are read-only (%v) because "+
						"the data plane never writes to a watched object",
						rule.Resources, verbs, want)
				}
				if slices.Contains(rule.APIGroups, "*") || slices.Contains(rule.Resources, "*") {
					t.Errorf("rule on %v uses a wildcard; a preset must enumerate what it grants "+
						"so `kubectl auth can-i --list` stays reviewable", rule.Resources)
				}
				if slices.Contains(rule.Resources, "secrets") {
					t.Error("preset grants secrets; v1/Secret is hard-denied as a watchable " +
						"kind in code (D8), so the grant is unreachable privilege")
				}
				if len(rule.ResourceNames) != 0 {
					t.Errorf("rule on %v pins resourceNames; a list/watch grant restricted by "+
						"name cannot serve an informer", rule.Resources)
				}
			}
		})
	}

	for _, want := range wantPresets {
		if !slices.Contains(found, want) {
			t.Errorf("preset %q is documented in docs/RBAC.md but does not ship", want)
		}
	}
}

// TestKustomizationWiresAggregationAndDefaultPreset asserts that the manifests
// above are actually installed. Each of these files is inert until listed here,
// and the failure mode of a missing line is silent: the install simply lacks a
// permission nobody notices until a rule degrades.
func TestKustomizationWiresAggregationAndDefaultPreset(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(rbacDir, "kustomization.yaml"))
	if err != nil {
		t.Fatalf("reading kustomization: %v", err)
	}
	kustomization := string(raw)

	required := map[string]string{
		"watcher_role.yaml":                "the aggregated watch role would not exist",
		"watcher_role_binding.yaml":        "the aggregated role would exist but grant the operator nothing",
		"manager_secret_role_binding.yaml": "sink credentials would be unreadable",
		"presets/core-workloads.yaml":      "the default install would have no watch rights at all",
	}
	for file, consequence := range required {
		if !strings.Contains(kustomization, "- "+file) {
			t.Errorf("config/rbac/kustomization.yaml does not include %s: %s", file, consequence)
		}
	}

	// The non-default presets must stay opt-in: shipping them all enabled would
	// make the default install's reach far wider than what it watches.
	for _, optional := range []string{"networking", "batch", "storage", "rbac-read"} {
		if strings.Contains(kustomization, "- presets/"+optional+".yaml") {
			t.Errorf("preset %q is enabled by default; only core-workloads ships enabled",
				optional)
		}
	}
}

// TestRBACDocsCarryTheFlatteningCaveat guards the documentation half of the task.
//
// The flattening caveat (D1) is the one thing about this model that a reader
// cannot discover from the manifests: ClickHouse read access ignores the
// namespace boundaries the write path respects. It is tagged with a literal,
// greppable marker so an operator can find it from a one-line search, and this
// test fails if the tag or the surrounding claim is edited away.
func TestRBACDocsCarryTheFlatteningCaveat(t *testing.T) {
	raw, err := os.ReadFile(docsRBAC)
	if err != nil {
		t.Fatalf("reading docs/RBAC.md: %v", err)
	}
	doc := string(raw)

	required := []string{
		// The greppable tag, in both its comment and its rendered form.
		"<!-- RBAC-FLATTENING-CAVEAT -->",
		"`[RBAC-FLATTENING-CAVEAT]`",
		// The claim itself must survive verbatim, not just the tag.
		"anyone who can query those tables can read the recorded state of every",
		"not shipped",
		// The other three things the model is unusable without.
		aggregateLabel,
		"kubestream-watcher",
		"No self-escalation",
	}
	for _, want := range required {
		if !strings.Contains(doc, want) {
			t.Errorf("docs/RBAC.md no longer contains %q", want)
		}
	}

	readme, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatalf("reading README: %v", err)
	}
	if !strings.Contains(string(readme), "docs/RBAC.md") {
		t.Error("README does not link docs/RBAC.md; the RBAC model is not discoverable")
	}
}
