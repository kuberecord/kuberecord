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

package chart

import (
	"bytes"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

// The two install paths must be the same install. The acceptance suite
// (test/e2e) runs against either one without a single assertion changing, and
// these tests are what makes that a property of the repository rather than a
// coincidence that held on the day it was tried.
//
// The comparison is deliberately about *substance*, not bytes: labels and
// annotations differ (Helm stamps its own, kustomize stamps `managed-by:
// kustomize`), and that difference is meaningless. Permissions, identities,
// arguments and probes are not.

// managerName is the Deployment, ServiceAccount and metrics-Service prefix both
// paths produce.
const managerName = "kubestream-controller-manager"

// kustomizeOnly are the two objects config/default ships and the chart does not,
// each for a reason:
//
//   - the Namespace: Helm's own `--create-namespace` owns that decision, and a
//     chart that templated a Namespace could not be installed into an existing one
//     without fighting ownership metadata.
//   - the credentials Secret: config/manager ships a `changeme` placeholder for
//     local use. A chart cannot: a password given as a value is stored in the
//     release and echoed by `helm get values`, so the chart requires the user to
//     create it (see the chart README).
var kustomizeOnly = []string{
	"Namespace/kubestream-system",
	"Secret/kubestream-clickhouse-credentials",
}

// TestRBACParityWithKustomize compares every Role, ClusterRole and binding the
// two paths produce. This is the test that fails when a kubebuilder RBAC marker
// changes: controller-gen regenerates config/rbac/role.yaml, the chart's copy of
// those rules does not follow, and a Helm-installed operator would be short a
// permission the kustomize-installed one has.
func TestRBACParityWithKustomize(t *testing.T) {
	chart := render(t, renderArgs{})
	kustomized := kustomizeDefault(t)

	// Same set of RBAC objects, by kind and name, in both directions.
	var chartRBAC, kustomizeRBAC []string
	for key, obj := range chart {
		if isRBACKey(obj) {
			chartRBAC = append(chartRBAC, key)
		}
	}
	for key, obj := range kustomized {
		if isRBACKey(obj) {
			kustomizeRBAC = append(kustomizeRBAC, key)
		}
	}
	sortStrings(chartRBAC)
	sortStrings(kustomizeRBAC)
	if !slices.Equal(chartRBAC, kustomizeRBAC) {
		t.Fatalf("RBAC object sets differ:\n  chart:     %v\n  kustomize: %v", chartRBAC, kustomizeRBAC)
	}

	for _, key := range kustomizeRBAC {
		obj := kustomized[key]
		t.Run(key, func(t *testing.T) {
			switch obj.kind {
			case kindRole, kindClusterRole:
				var want, got rbacv1.ClusterRole // ClusterRole decodes a Role's rules too
				obj.decode(t, &want)
				chart[key].decode(t, &got)
				assertRulesEqual(t, key, want.Rules, got.Rules)
				assertAggregationEqual(t, key, want.AggregationRule, got.AggregationRule)
			case kindRoleBinding, kindClusterRoleBinding:
				var want, got rbacv1.ClusterRoleBinding
				obj.decode(t, &want)
				chart[key].decode(t, &got)
				if want.RoleRef != got.RoleRef {
					t.Errorf("%s roleRef:\n got %+v\nwant %+v", key, got.RoleRef, want.RoleRef)
				}
				if !slices.Equal(want.Subjects, got.Subjects) {
					t.Errorf("%s subjects:\n got %+v\nwant %+v", key, got.Subjects, want.Subjects)
				}
			}
		})
	}
}

// TestWorkloadParityWithKustomize compares what the operator actually runs as:
// its identity, arguments, environment, ports, probes and security context. A
// difference in any of these is a difference in behaviour between the two install
// paths, which is what would make "the happy path passes unmodified" stop being
// true.
//
// Resources are deliberately *not* compared: sizing the operator for the cluster
// it runs on is exactly what a chart value is for (see docs/PERFORMANCE.md), and
// the two paths ship the same small-profile defaults only by coincidence of
// today's numbers.
func TestWorkloadParityWithKustomize(t *testing.T) {
	chart := render(t, renderArgs{})
	kustomized := kustomizeDefault(t)

	var want, got deploymentSpec
	kustomized.get(t, kindDeployment, managerName).decode(t, &want)
	chart.get(t, kindDeployment, managerName).decode(t, &got)

	if want.Spec.Selector.MatchLabels == nil {
		t.Fatal("the kustomize Deployment has no selector; the comparison below would be vacuous")
	}
	if !mapsEqual(want.Spec.Selector.MatchLabels, got.Spec.Selector.MatchLabels) {
		t.Errorf("pod selector:\n got %v\nwant %v",
			got.Spec.Selector.MatchLabels, want.Spec.Selector.MatchLabels)
	}
	if want.Spec.Template.Spec.ServiceAccountName != got.Spec.Template.Spec.ServiceAccountName {
		t.Errorf("serviceAccountName: got %q, want %q",
			got.Spec.Template.Spec.ServiceAccountName, want.Spec.Template.Spec.ServiceAccountName)
	}
	if want.Spec.Template.Spec.TerminationGracePeriodSeconds !=
		got.Spec.Template.Spec.TerminationGracePeriodSeconds {
		t.Errorf("terminationGracePeriodSeconds: got %d, want %d",
			got.Spec.Template.Spec.TerminationGracePeriodSeconds,
			want.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	assertYAMLEqual(t, "pod securityContext",
		want.Spec.Template.Spec.SecurityContext, got.Spec.Template.Spec.SecurityContext)

	wantContainer := managerContainer(t, kustomized.get(t, kindDeployment, managerName))
	gotContainer := managerContainer(t, chart.get(t, kindDeployment, managerName))

	if !slices.Equal(wantContainer.Command, gotContainer.Command) {
		t.Errorf("command: got %v, want %v", gotContainer.Command, wantContainer.Command)
	}
	// Argument *order* is not meaningful (the kustomize metrics patch prepends
	// its flag), so the sets are compared.
	wantArgs, gotArgs := slices.Clone(wantContainer.Args), slices.Clone(gotContainer.Args)
	sortStrings(wantArgs)
	sortStrings(gotArgs)
	if !slices.Equal(wantArgs, gotArgs) {
		t.Errorf("manager args:\n got %v\nwant %v", gotArgs, wantArgs)
	}
	assertYAMLEqual(t, "container securityContext",
		wantContainer.SecurityContext, gotContainer.SecurityContext)
	assertYAMLEqual(t, "livenessProbe", wantContainer.LivenessProbe, gotContainer.LivenessProbe)
	assertYAMLEqual(t, "readinessProbe", wantContainer.ReadinessProbe, gotContainer.ReadinessProbe)

	// The environment is compared by name and value, since POD_NAMESPACE's value
	// comes from the downward API and CLUSTER_ID is stamped on every row written.
	assertEnvEqual(t, wantContainer, gotContainer)

	// The health port has to agree with the probes on both paths; metrics is
	// compared through the Service below.
	if !slices.ContainsFunc(gotContainer.Ports, func(p struct {
		Name          string `json:"name"`
		ContainerPort int    `json:"containerPort"`
	}) bool {
		return p.Name == "health" && p.ContainerPort == gotContainer.LivenessProbe.HTTPGet.Port
	}) {
		t.Errorf("no health containerPort matching the liveness probe: %+v", gotContainer.Ports)
	}
}

// TestMetricsServiceParityWithKustomize asserts a scrape configuration written
// for one install path works against the other: same Service name, same port,
// same selector.
func TestMetricsServiceParityWithKustomize(t *testing.T) {
	chart := render(t, renderArgs{})
	kustomized := kustomizeDefault(t)

	type service struct {
		Spec struct {
			Ports []struct {
				Name       string `json:"name"`
				Port       int    `json:"port"`
				TargetPort int    `json:"targetPort"`
				Protocol   string `json:"protocol"`
			} `json:"ports"`
			Selector map[string]string `json:"selector"`
		} `json:"spec"`
	}
	var want, got service
	name := managerName + "-metrics-service"
	kustomized.get(t, kindService, name).decode(t, &want)
	chart.get(t, kindService, name).decode(t, &got)

	assertYAMLEqual(t, "metrics Service ports", want.Spec.Ports, got.Spec.Ports)
	if !mapsEqual(want.Spec.Selector, got.Spec.Selector) {
		t.Errorf("metrics Service selector:\n got %v\nwant %v", got.Spec.Selector, want.Spec.Selector)
	}
}

// TestNonRBACObjectParityWithKustomize asserts neither path installs an object
// the other does not, beyond the two documented exceptions. It is the test that
// notices a whole component (a NetworkPolicy, a ServiceMonitor, a PodDisruption
// Budget) appearing on one path only.
func TestNonRBACObjectParityWithKustomize(t *testing.T) {
	chart := render(t, renderArgs{})
	kustomized := kustomizeDefault(t)

	for _, key := range kustomized.keys() {
		if slices.Contains(kustomizeOnly, key) || isRBACKey(kustomized[key]) {
			continue
		}
		if _, found := chart[key]; !found {
			t.Errorf("config/default installs %s; the chart does not", key)
		}
	}
	for _, key := range chart.keys() {
		if isRBACKey(chart[key]) {
			continue
		}
		if _, found := kustomized[key]; !found {
			t.Errorf("the chart installs %s; config/default does not", key)
		}
	}

	// The exceptions are asserted rather than merely excluded, so that this test
	// starts failing the day one of them stops being an exception.
	for _, key := range kustomizeOnly {
		if _, found := kustomized[key]; !found {
			t.Errorf("%s is listed as kustomize-only but config/default no longer ships it", key)
		}
		if _, found := chart[key]; found {
			t.Errorf("%s is listed as kustomize-only but the chart now renders it", key)
		}
	}
}

// TestChartCRDsMatchConfig asserts the chart's crds/ copies are byte-identical to
// the generated CRDs. Helm requires them inside the chart, so a copy is
// unavoidable; a *stale* copy is not, and it is the worst kind of staleness — the
// operator's code would validate fields the installed CRD rejects.
func TestChartCRDsMatchConfig(t *testing.T) {
	assertDirectoriesIdentical(t, configCRDDir, chartDir+"/crds")
}

// TestChartPresetFilesMatchConfig asserts the same for the preset files the
// chart's templates read at render time.
func TestChartPresetFilesMatchConfig(t *testing.T) {
	assertDirectoriesIdentical(t, configPresets, chartDir+"/files/presets")
}

// assertDirectoriesIdentical compares two repo-relative directories of YAML files
// by name and by content, naming `make helm-sync` as the fix.
func assertDirectoriesIdentical(t *testing.T, sourceDir, copyDir string) {
	t.Helper()
	sources, copies := listYAML(t, sourceDir), listYAML(t, copyDir)
	if !slices.Equal(sources, copies) {
		t.Fatalf("%s holds %v but %s holds %v — run `make helm-sync`",
			sourceDir, sources, copyDir, copies)
	}
	for _, name := range sources {
		if !bytes.Equal(readFile(t, sourceDir+"/"+name), readFile(t, copyDir+"/"+name)) {
			t.Errorf("%s/%s differs from %s/%s — run `make helm-sync`", copyDir, name, sourceDir, name)
		}
	}
}

func isRBACKey(obj object) bool {
	switch obj.kind {
	case kindRole, kindClusterRole, kindRoleBinding, kindClusterRoleBinding:
		return true
	}
	return false
}

// assertAggregationEqual compares two aggregation rules. It matters for exactly
// one object — the watcher role — and it matters a lot: an aggregated role whose
// selector drifted would collect no presets and grant nothing.
func assertAggregationEqual(t *testing.T, subject string, want, got *rbacv1.AggregationRule) {
	t.Helper()
	switch {
	case want == nil && got == nil:
		return
	case want == nil || got == nil:
		t.Errorf("%s aggregationRule: got %+v, want %+v", subject, got, want)
		return
	}
	assertYAMLEqual(t, subject+" aggregationRule", want.ClusterRoleSelectors, got.ClusterRoleSelectors)
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, valueA := range a {
		if valueB, found := b[key]; !found || valueA != valueB {
			return false
		}
	}
	return true
}
