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
	"fmt"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// aggregateLabel is the label that makes a ClusterRole part of the operator's
// watch rights. A preset without it is inert, which is the one way a rendered
// preset can look right and grant nothing.
const aggregateLabel = "kuberecord.io/aggregate-to-watcher"

// TestDefaultValuesRenderTheExpectedObjects pins the default install's object
// set. It is the test that notices a template being added, removed or silently
// disabled — every other test here asserts about one value at a time.
func TestDefaultValuesRenderTheExpectedObjects(t *testing.T) {
	rendered := render(t, renderArgs{})

	want := []string{
		"ClusterRole/kuberecord-manager-role",
		"ClusterRole/kuberecord-metrics-auth-role",
		"ClusterRole/kuberecord-metrics-reader",
		"ClusterRole/kuberecord-watcher",
		"ClusterRole/kuberecord-watcher-core-workloads",
		"ClusterRoleBinding/kuberecord-manager-rolebinding",
		"ClusterRoleBinding/kuberecord-metrics-auth-rolebinding",
		"ClusterRoleBinding/kuberecord-watcher-rolebinding",
		"CustomResourceDefinition/clickhousesinks.kuberecord.io",
		"CustomResourceDefinition/clusterstreamrules.kuberecord.io",
		"CustomResourceDefinition/s3sinks.kuberecord.io",
		"CustomResourceDefinition/streamrules.kuberecord.io",
		"Deployment/kuberecord-controller-manager",
		"Role/kuberecord-leader-election-role",
		"Role/kuberecord-manager-role",
		"RoleBinding/kuberecord-leader-election-rolebinding",
		"RoleBinding/kuberecord-manager-secret-rolebinding",
		"Service/kuberecord-controller-manager-metrics-service",
		"ServiceAccount/kuberecord-controller-manager",
	}
	if got := rendered.keys(); !slices.Equal(got, want) {
		t.Errorf("default rendering:\n got %v\nwant %v", got, want)
	}

	// The chart creates no Secret, at any values. A password in a chart would end
	// up in the release's stored manifest and in every `helm get values` output.
	for _, obj := range rendered {
		if obj.kind == "Secret" {
			t.Errorf("the chart must never render a Secret; found %s", obj)
		}
	}
}

// TestPresetToggleAddsAndRemovesExactlyThatClusterRole is the Task 2.4 acceptance
// criterion: toggling one preset value changes exactly one object.
//
// Both directions are asserted for every shipped preset, because the two failure
// modes are different: a preset that renders when disabled hands the operator
// standing rights nobody asked for, and one that does not render when enabled
// leaves rules permanently degraded with no hint as to why.
func TestPresetToggleAddsAndRemovesExactlyThatClusterRole(t *testing.T) {
	presets := presetNames(t)
	// The "everything off" baseline every case is measured against, so that
	// "exactly that ClusterRole" is a claim about a diff rather than a count.
	allOff := make([]string, 0, len(presets))
	for _, preset := range presets {
		allOff = append(allOff, fmt.Sprintf("rbac.presets.%s=false", preset))
	}
	baseline := render(t, renderArgs{sets: allOff})
	for _, preset := range presets {
		if name := presetRoleName(preset); baseline.has(kindClusterRole, name) {
			t.Fatalf("with every preset disabled, %s must not be rendered", name)
		}
	}

	for _, preset := range presets {
		t.Run(preset, func(t *testing.T) {
			enabled := render(t, renderArgs{
				sets: append(slices.Clone(allOff), fmt.Sprintf("rbac.presets.%s=true", preset)),
			})

			want := presetRoleName(preset)
			added := diffKeys(baseline.keys(), enabled.keys())
			if !slices.Equal(added, []string{kindClusterRole + "/" + want}) {
				t.Fatalf("enabling %s added %v; want exactly [%s/%s]", preset, added, kindClusterRole, want)
			}
			if removed := diffKeys(enabled.keys(), baseline.keys()); len(removed) != 0 {
				t.Errorf("enabling %s removed %v; want nothing removed", preset, removed)
			}

			// A rendered preset that is not labelled for aggregation grants
			// nothing at all, so the label is part of "the ClusterRole was added".
			role := enabled.get(t, kindClusterRole, want)
			if got := labelsOf(t, role)[aggregateLabel]; got != "true" {
				t.Errorf("%s carries %s=%q; want \"true\"", want, aggregateLabel, got)
			}
		})
	}
}

// TestPresetRulesMatchConfigPresets asserts the chart grants exactly what
// config/rbac/presets/ grants — the same rules, in the same order, for the same
// role name. The chart reads those files at render time, so this is the test that
// keeps that indirection honest: a stale copy under the chart's files/ shows up
// here as a rule diff.
func TestPresetRulesMatchConfigPresets(t *testing.T) {
	presets := presetNames(t)
	all := make([]string, 0, len(presets))
	for _, preset := range presets {
		all = append(all, fmt.Sprintf("rbac.presets.%s=true", preset))
	}
	rendered := render(t, renderArgs{sets: all})

	for _, preset := range presets {
		t.Run(preset, func(t *testing.T) {
			var source rbacv1.ClusterRole
			if err := yaml.Unmarshal(readFile(t, configPresets+"/"+preset+".yaml"), &source); err != nil {
				t.Fatalf("parsing config preset %s: %v", preset, err)
			}

			// The chart prefixes the source file's own object name, so a preset
			// applied by kubectl and one installed by Helm aggregate as the same
			// grant under different names.
			wantName := chartRelease + "-" + source.Name
			if wantName != presetRoleName(preset) {
				t.Fatalf("preset %s declares name %q; the chart's naming assumes watcher-%s",
					preset, source.Name, preset)
			}

			var got rbacv1.ClusterRole
			rendered.get(t, kindClusterRole, wantName).decode(t, &got)
			assertRulesEqual(t, wantName, source.Rules, got.Rules)
		})
	}
}

// TestPresetValuesCoverEveryShippedPreset makes values.yaml complete: every file
// in config/rbac/presets/ is offered as a value, and no value names a preset that
// does not exist. Without this a preset added to config/ would be invisible to
// Helm users, and a typo in values.yaml would be a render-time surprise.
func TestPresetValuesCoverEveryShippedPreset(t *testing.T) {
	var values struct {
		RBAC struct {
			Presets map[string]bool `json:"presets"`
		} `json:"rbac"`
	}
	if err := yaml.Unmarshal(readFile(t, chartDir+"/values.yaml"), &values); err != nil {
		t.Fatalf("parsing values.yaml: %v", err)
	}

	offered := make([]string, 0, len(values.RBAC.Presets))
	for preset := range values.RBAC.Presets {
		offered = append(offered, preset)
	}
	sortStrings(offered)
	if want := presetNames(t); !slices.Equal(offered, want) {
		t.Errorf("values.yaml offers presets %v; config/rbac/presets ships %v", offered, want)
	}

	// core-workloads is the one preset enabled by default (it is what the shipped
	// sample rules watch); everything else is opt-in.
	for preset, enabled := range values.RBAC.Presets {
		if want := preset == "core-workloads"; enabled != want {
			t.Errorf("values.yaml default for preset %s is %v; want %v", preset, enabled, want)
		}
	}
}

// TestUnknownPresetFailsRendering asserts a misspelled preset is a loud render
// failure rather than a silently missing grant — the difference between "helm
// install told me" and "every rule is degraded and I do not know why".
func TestUnknownPresetFailsRendering(t *testing.T) {
	out, err := renderRaw(t, renderArgs{sets: []string{"rbac.presets.no-such-preset=true"}})
	if err == nil {
		t.Fatalf("rendering with an unknown preset succeeded; want failure\n%s", out)
	}
	if !strings.Contains(out, "no-such-preset") {
		t.Errorf("failure message does not name the offending preset:\n%s", out)
	}
}

// TestRBACCreateFalseRemovesEveryRoleAndBinding covers the platform-managed-RBAC
// case: the switch has to remove *all* of it, since a half-installed grant is
// harder to reason about than none.
func TestRBACCreateFalseRemovesEveryRoleAndBinding(t *testing.T) {
	rendered := render(t, renderArgs{sets: []string{"rbac.create=false"}})
	for _, obj := range rendered {
		switch obj.kind {
		case kindClusterRole, kindClusterRoleBinding, kindRole, kindRoleBinding:
			t.Errorf("rbac.create=false still rendered %s", obj)
		}
	}
	// The workload itself is untouched: the operator is still installed, it simply
	// has no permissions of the chart's making.
	if !rendered.has(kindDeployment, "kuberecord-controller-manager") {
		t.Error("rbac.create=false must not remove the manager Deployment")
	}
}

// TestMetricsToggle asserts the metrics endpoint is one decision, not four: the
// Service, the endpoint's authn/authz roles and the manager's own argument all
// follow `metrics.enabled` together.
func TestMetricsToggle(t *testing.T) {
	metricsObjects := []struct{ kind, name string }{
		{kindService, "kuberecord-controller-manager-metrics-service"},
		{kindClusterRole, "kuberecord-metrics-auth-role"},
		{kindClusterRoleBinding, "kuberecord-metrics-auth-rolebinding"},
		{kindClusterRole, "kuberecord-metrics-reader"},
	}

	t.Run("enabled", func(t *testing.T) {
		rendered := render(t, renderArgs{})
		for _, want := range metricsObjects {
			if !rendered.has(want.kind, want.name) {
				t.Errorf("metrics enabled: expected %s/%s", want.kind, want.name)
			}
		}
		args := containerArgs(t, rendered.get(t, kindDeployment, "kuberecord-controller-manager"))
		if !hasArg(args, "--metrics-bind-address=:8443", false) {
			t.Errorf("metrics enabled: args %v carry no --metrics-bind-address=:8443", args)
		}
		// Secure is the default, and the binary's default too, so the chart passes
		// no --metrics-secure at all rather than restating it.
		if hasArg(args, "--metrics-secure", true) {
			t.Errorf("metrics enabled and secure: args %v should not restate --metrics-secure", args)
		}
	})

	t.Run("disabled", func(t *testing.T) {
		rendered := render(t, renderArgs{sets: []string{"metrics.enabled=false"}})
		for _, gone := range metricsObjects {
			if rendered.has(gone.kind, gone.name) {
				t.Errorf("metrics disabled: %s/%s should not be rendered", gone.kind, gone.name)
			}
		}
		deployment := rendered.get(t, kindDeployment, "kuberecord-controller-manager")
		if args := containerArgs(t, deployment); hasArg(args, "--metrics-bind-address", true) {
			t.Errorf("metrics disabled: args %v still bind the metrics endpoint", args)
		}
		for _, port := range managerContainer(t, deployment).Ports {
			if port.Name == "metrics" {
				t.Error("metrics disabled: the container still declares a metrics port")
			}
		}
	})

	t.Run("insecure", func(t *testing.T) {
		rendered := render(t, renderArgs{sets: []string{"metrics.secure=false"}})
		args := containerArgs(t, rendered.get(t, kindDeployment, "kuberecord-controller-manager"))
		if !hasArg(args, "--metrics-secure=false", false) {
			t.Errorf("metrics.secure=false: args %v do not turn TLS off", args)
		}
	})
}

// TestLeaderElectionToggle asserts the flag and the lease permissions move
// together: leader election without the Role is a crash-loop on startup, and the
// Role without the flag is a permission nothing uses.
func TestLeaderElectionToggle(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		rendered := render(t, renderArgs{})
		if !rendered.has(kindRole, "kuberecord-leader-election-role") {
			t.Error("leader election enabled: expected the leader-election Role")
		}
		if !rendered.has(kindRoleBinding, "kuberecord-leader-election-rolebinding") {
			t.Error("leader election enabled: expected the leader-election RoleBinding")
		}
		args := containerArgs(t, rendered.get(t, kindDeployment, "kuberecord-controller-manager"))
		if !hasArg(args, "--leader-elect", false) {
			t.Errorf("leader election enabled: args %v carry no --leader-elect", args)
		}
	})

	t.Run("disabled", func(t *testing.T) {
		rendered := render(t, renderArgs{sets: []string{"leaderElection.enabled=false"}})
		if rendered.has(kindRole, "kuberecord-leader-election-role") {
			t.Error("leader election disabled: the leader-election Role should not be rendered")
		}
		if rendered.has(kindRoleBinding, "kuberecord-leader-election-rolebinding") {
			t.Error("leader election disabled: the leader-election RoleBinding should not be rendered")
		}
		args := containerArgs(t, rendered.get(t, kindDeployment, "kuberecord-controller-manager"))
		if hasArg(args, "--leader-elect", false) {
			t.Errorf("leader election disabled: args %v still pass --leader-elect", args)
		}
	})
}

// TestReplicasRequireLeaderElection asserts the one value combination that would
// install a *silently wrong* operator — two active instances writing every row
// twice — is refused at render time.
func TestReplicasRequireLeaderElection(t *testing.T) {
	out, err := renderRaw(t, renderArgs{sets: []string{
		"replicaCount=2", "leaderElection.enabled=false",
	}})
	if err == nil {
		t.Fatalf("replicaCount=2 without leader election rendered successfully; want failure\n%s", out)
	}
	if !strings.Contains(out, "leaderElection.enabled") {
		t.Errorf("failure message does not point at the value to fix:\n%s", out)
	}
}

// TestImageReference covers the three ways an install names its operator image.
func TestImageReference(t *testing.T) {
	const digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	tests := []struct {
		name string
		sets []string
		want string
	}{
		{
			// An empty tag follows the chart's appVersion, so upgrading the chart
			// moves the operator with it.
			name: "tag defaults to appVersion",
			want: "ghcr.io/yelzhy/kuberecord:" + chartAppVersion(t),
		},
		{
			name: "explicit repository and tag",
			sets: []string{"image.repository=example.com/kuberecord", "image.tag=v9.9.9"},
			want: "example.com/kuberecord:v9.9.9",
		},
		{
			// A digest pins by content, so it must win over any tag that is also set.
			name: "digest wins over tag",
			sets: []string{"image.tag=v9.9.9", "image.digest=" + digest},
			want: "ghcr.io/yelzhy/kuberecord@" + digest,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rendered := render(t, renderArgs{sets: tc.sets})
			got := managerContainer(t, rendered.get(t, kindDeployment, "kuberecord-controller-manager"))
			if got.Image != tc.want {
				t.Errorf("image = %q; want %q", got.Image, tc.want)
			}
		})
	}
}

// TestExtraLabelsReachEveryObjectButNoSelector asserts extra labels are applied
// where they are useful and kept out of the two places they would be harmful: a
// Deployment's selector is immutable, so folding user labels into it would make
// the *next* `helm upgrade` fail, and the metrics Service's selector must keep
// matching pods from before the labels were added.
func TestExtraLabelsReachEveryObjectButNoSelector(t *testing.T) {
	rendered := render(t, renderArgs{sets: []string{"extraLabels.team=platform"}})

	for _, obj := range rendered {
		if obj.kind == kindCRD {
			// CRDs live in crds/ and are applied verbatim by Helm: they are not
			// templated, so no chart label can reach them.
			continue
		}
		if got := labelsOf(t, obj)[teamLabel]; got != "platform" {
			t.Errorf("%s carries %s=%q; want \"platform\"", obj, teamLabel, got)
		}
	}

	var deployment deploymentSpec
	rendered.get(t, kindDeployment, "kuberecord-controller-manager").decode(t, &deployment)
	if _, found := deployment.Spec.Selector.MatchLabels[teamLabel]; found {
		t.Error("extra labels must not appear in the Deployment's immutable selector")
	}
	if _, found := deployment.Spec.Template.Metadata.Labels[teamLabel]; !found {
		t.Error("extra labels should appear on the pod template")
	}

	var service struct {
		Spec struct {
			Selector map[string]string `json:"selector"`
		} `json:"spec"`
	}
	rendered.get(t, kindService, "kuberecord-controller-manager-metrics-service").decode(t, &service)
	if _, found := service.Spec.Selector[teamLabel]; found {
		t.Error("extra labels must not appear in the metrics Service's selector")
	}
}

// teamLabel is the arbitrary user label TestExtraLabels… threads through.
const teamLabel = "team"

// TestDefaultSinkToggle covers the optional starter sink: off by default,
// complete when on, and refused rather than rendered half-configured.
func TestDefaultSinkToggle(t *testing.T) {
	const addr = "clickhouse.kuberecord-system.svc:9000"

	t.Run("off by default", func(t *testing.T) {
		rendered := render(t, renderArgs{})
		if rendered.has(kindClickHouseSink, "default") {
			t.Error("createDefaultSink defaults to false; no ClickHouseSink should be rendered")
		}
	})

	t.Run("on", func(t *testing.T) {
		rendered := render(t, renderArgs{sets: []string{
			"createDefaultSink=true",
			"defaultSink.connection.addr=" + addr,
		}})
		var sink struct {
			Spec struct {
				Connection struct {
					Addr                 string `json:"addr"`
					Database             string `json:"database"`
					Username             string `json:"username"`
					CredentialsSecretRef struct {
						Name      string `json:"name"`
						Namespace string `json:"namespace"`
					} `json:"credentialsSecretRef"`
				} `json:"connection"`
				Writer map[string]any `json:"writer"`
				Policy map[string]any `json:"policy"`
			} `json:"spec"`
		}
		rendered.get(t, kindClickHouseSink, "default").decode(t, &sink)

		if sink.Spec.Connection.Addr != addr {
			t.Errorf("addr = %q; want %q", sink.Spec.Connection.Addr, addr)
		}
		if sink.Spec.Connection.CredentialsSecretRef.Name != "clickhouse-credentials" {
			t.Errorf("credentialsSecretRef.name = %q", sink.Spec.Connection.CredentialsSecretRef.Name)
		}
		// No namespace: it defaults to the operator's own, the only namespace the
		// operator holds Secret rights in. Spelling one here would invite pointing
		// a cluster-scoped sink at a Secret the operator cannot read.
		if ns := sink.Spec.Connection.CredentialsSecretRef.Namespace; ns != "" {
			t.Errorf("credentialsSecretRef.namespace = %q; want it omitted", ns)
		}
		// Unset sections are omitted rather than rendered empty, so the CRD's own
		// defaults apply to them.
		if len(sink.Spec.Writer) != 0 || len(sink.Spec.Policy) != 0 {
			t.Errorf("unset writer/policy should be omitted; got writer=%v policy=%v",
				sink.Spec.Writer, sink.Spec.Policy)
		}
	})

	t.Run("requires an address", func(t *testing.T) {
		out, err := renderRaw(t, renderArgs{sets: []string{"createDefaultSink=true"}})
		if err == nil {
			t.Fatalf("createDefaultSink=true with no addr rendered successfully; want failure\n%s", out)
		}
		if !strings.Contains(out, "defaultSink.connection.addr") {
			t.Errorf("failure message does not name the missing value:\n%s", out)
		}
	})
}

// TestCIValuesFilesRender asserts the two shipped ci/ values files — the smallest
// and the largest install the chart supports — still render. They are what `make
// helm-lint` and `make helm-kubeconform` validate, so a template that only works
// at default values fails here first.
func TestCIValuesFilesRender(t *testing.T) {
	for _, file := range listYAML(t, chartDir+"/ci") {
		t.Run(file, func(t *testing.T) {
			rendered := render(t, renderArgs{valuesFiles: []string{chartDir + "/ci/" + file}})
			if !rendered.has(kindDeployment, "kuberecord-controller-manager") {
				t.Errorf("ci/%s renders no manager Deployment", file)
			}
		})
	}
}

// TestChartVersionsMatchMakefile keeps the release-bump promise in the Makefile
// honest: VERSION there, and the chart's version/appVersion, are one number.
func TestChartVersionsMatchMakefile(t *testing.T) {
	var makefileVersion string
	for line := range strings.SplitSeq(string(readFile(t, "Makefile")), "\n") {
		if rest, found := strings.CutPrefix(line, "VERSION ?= "); found {
			makefileVersion = strings.TrimSpace(rest)
			break
		}
	}
	if makefileVersion == "" {
		t.Fatal("no `VERSION ?= ` line found in the Makefile")
	}

	var chart struct {
		Version    string `json:"version"`
		AppVersion string `json:"appVersion"`
	}
	if err := yaml.Unmarshal(readFile(t, chartDir+"/Chart.yaml"), &chart); err != nil {
		t.Fatalf("parsing Chart.yaml: %v", err)
	}
	if chart.Version != makefileVersion {
		t.Errorf("Chart.yaml version = %q; Makefile VERSION = %q", chart.Version, makefileVersion)
	}
	if want := "v" + makefileVersion; chart.AppVersion != want {
		t.Errorf("Chart.yaml appVersion = %q; want %q", chart.AppVersion, want)
	}
}

// chartAppVersion reads the appVersion the chart declares, so tests assert
// against the chart rather than against a copy of its version.
func chartAppVersion(t *testing.T) string {
	t.Helper()
	var chart struct {
		AppVersion string `json:"appVersion"`
	}
	if err := yaml.Unmarshal(readFile(t, chartDir+"/Chart.yaml"), &chart); err != nil {
		t.Fatalf("parsing Chart.yaml: %v", err)
	}
	return chart.AppVersion
}

// diffKeys returns the keys in b that are not in a.
func diffKeys(a, b []string) []string {
	var added []string
	for _, key := range b {
		if !slices.Contains(a, key) {
			added = append(added, key)
		}
	}
	return added
}

// assertRulesEqual compares two rule sets element by element, reporting the first
// difference in full — a rule diff is exactly the kind of failure that is
// unreadable when reported as two whole objects.
func assertRulesEqual(t *testing.T, subject string, want, got []rbacv1.PolicyRule) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: %d rules; want %d\n got %+v\nwant %+v", subject, len(got), len(want), got, want)
	}
	for i := range want {
		if !rulesEqual(want[i], got[i]) {
			t.Errorf("%s: rule %d\n got %+v\nwant %+v", subject, i, got[i], want[i])
		}
	}
}

func rulesEqual(a, b rbacv1.PolicyRule) bool {
	return slices.Equal(a.APIGroups, b.APIGroups) &&
		slices.Equal(a.Resources, b.Resources) &&
		slices.Equal(a.ResourceNames, b.ResourceNames) &&
		slices.Equal(a.NonResourceURLs, b.NonResourceURLs) &&
		slices.Equal(a.Verbs, b.Verbs)
}
