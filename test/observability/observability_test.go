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

// Package observability validates the shipped Grafana dashboard and Prometheus
// alert rules (Task 2.5).
//
// A dashboard is a text file that nothing compiles, so the failure mode it invites
// is silence: a metric gets renamed, a panel keeps its title, and the graph is
// simply empty the day someone finally looks at it. These tests close that gap
// from three directions — the artifacts are structurally valid (JSON Schema), they
// still contain the panels and alerts the acceptance criteria call for, and every
// metric they query is one the operator's collectors actually declare, checked
// against the collectors themselves rather than against a hand-kept list.
package observability

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"sigs.k8s.io/yaml"

	"github.com/yelzhy/kubestream/internal/controller"
	"github.com/yelzhy/kubestream/internal/pipeline"
)

// Paths are resolved relative to this file rather than to the working directory,
// so the suite behaves the same under `go test ./...` from the repo root and under
// an editor that runs one test from the package directory.
var (
	dashboardPath       = repoPath("deploy", "grafana", "operator-health.json")
	dashboardSchemaPath = repoPath("deploy", "grafana", "dashboard.schema.json")
	alertsPath          = repoPath("deploy", "prometheus", "alerts.yaml")
	alertsSchemaPath    = repoPath("deploy", "prometheus", "prometheusrule.schema.json")
)

func repoPath(elems ...string) string {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot locate the test source file")
	}
	root := filepath.Join(filepath.Dir(thisFile), "..", "..")
	return filepath.Join(append([]string{root}, elems...)...)
}

// TestArtifactsMatchTheirSchemas is the acceptance criteria's JSON-schema check.
// It runs in `make test`, and therefore in CI, on every push.
func TestArtifactsMatchTheirSchemas(t *testing.T) {
	tests := []struct {
		name       string
		schemaPath string
		docPath    string
		// yamlDoc marks a document that must be converted to JSON first. JSON
		// Schema validates a decoded document, not a syntax, so a YAML file is
		// validated by decoding it through the same YAML-to-JSON path the
		// Kubernetes API server uses.
		yamlDoc bool
	}{
		{
			name:       "grafana dashboard",
			schemaPath: dashboardSchemaPath,
			docPath:    dashboardPath,
		},
		{
			name:       "prometheus alert rules",
			schemaPath: alertsSchemaPath,
			docPath:    alertsPath,
			yamlDoc:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := compileSchema(t, tt.schemaPath)
			doc := readDocument(t, tt.docPath, tt.yamlDoc)
			if err := schema.Validate(doc); err != nil {
				// The default error rendering is a one-line summary; the detailed
				// form names the failing instance location, which is the only
				// useful thing when a nested panel is wrong.
				t.Fatalf("%s does not match %s:\n%s",
					rel(tt.docPath), rel(tt.schemaPath), explain(err))
			}
		})
	}
}

// TestDashboardHasTheRequiredPanels pins the panel set to the acceptance criteria.
//
// It matches on the query rather than on the title: a title is a label somebody
// may reword, while the query is the panel. A panel that stops asking the
// question it was added for is the failure this catches, whatever it is called.
func TestDashboardHasTheRequiredPanels(t *testing.T) {
	dash := decodeDashboard(t)

	tests := []struct {
		// panel is the criterion, named as the acceptance criteria name it.
		panel string
		// wantExprs are substrings that must each appear in some query of one
		// single panel — the panel must ask all of them, not the dashboard.
		wantExprs []string
	}{
		{
			panel: "queue depth vs capacity",
			wantExprs: []string{
				"kubestream_write_queue_depth",
				"kubestream_write_queue_capacity",
			},
		},
		{
			panel:     "write outcomes rate",
			wantExprs: []string{"rate(kubestream_writes_total"},
		},
		{
			panel: "write latency p99",
			wantExprs: []string{
				"histogram_quantile(0.99",
				"kubestream_write_latency_seconds_bucket",
			},
		},
		{
			panel:     "batch-size distribution",
			wantExprs: []string{"kubestream_write_batch_rows_bucket"},
		},
		{
			panel:     "retry rate",
			wantExprs: []string{"rate(kubestream_write_retry_attempts_total"},
		},
		{
			panel:     "degraded rules count",
			wantExprs: []string{`kubestream_rules{condition="Ready",status="false"}`},
		},
		{
			panel:     "SafeMode scopes",
			wantExprs: []string{"kubestream_safe_mode"},
		},
		{
			panel:     "enqueue backpressure",
			wantExprs: []string{"kubestream_enqueue_timeouts_total"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.panel, func(t *testing.T) {
			for _, panel := range dash.Panels {
				joined := strings.Join(panel.exprs(), "\n")
				if containsAll(joined, tt.wantExprs) {
					return
				}
			}
			t.Errorf("no panel queries all of %q; the %q criterion is not covered", tt.wantExprs, tt.panel)
		})
	}
}

// TestDashboardPanelsAreWellFormed covers the mistakes a schema cannot see:
// duplicate ids (Grafana silently drops the second), panels that overlap on the
// grid, and datasources that do not go through the dashboard's variable.
func TestDashboardPanelsAreWellFormed(t *testing.T) {
	dash := decodeDashboard(t)

	const wantDatasourceUID = "${datasource}"
	seenIDs := map[int]string{}
	seenTitles := map[string]bool{}
	occupied := map[[2]int]string{}

	for _, panel := range dash.Panels {
		if other, dup := seenIDs[panel.ID]; dup {
			t.Errorf("panel %q reuses id %d, already held by %q", panel.Title, panel.ID, other)
		}
		seenIDs[panel.ID] = panel.Title

		if seenTitles[panel.Title] {
			t.Errorf("panel title %q appears twice; titles are how the docs refer to panels", panel.Title)
		}
		seenTitles[panel.Title] = true

		if panel.Datasource.UID != wantDatasourceUID {
			t.Errorf("panel %q reads datasource uid %q, want %q — a pinned uid imports as a broken panel",
				panel.Title, panel.Datasource.UID, wantDatasourceUID)
		}
		if panel.GridPos.X+panel.GridPos.W > 24 {
			t.Errorf("panel %q runs past the 24-column grid (x=%d w=%d)",
				panel.Title, panel.GridPos.X, panel.GridPos.W)
		}
		for x := panel.GridPos.X; x < panel.GridPos.X+panel.GridPos.W; x++ {
			for y := panel.GridPos.Y; y < panel.GridPos.Y+panel.GridPos.H; y++ {
				if other, taken := occupied[[2]int{x, y}]; taken {
					t.Errorf("panel %q overlaps %q at (%d,%d)", panel.Title, other, x, y)
				}
				occupied[[2]int{x, y}] = panel.Title
			}
		}

		for _, target := range panel.Targets {
			if target.Datasource.UID != wantDatasourceUID {
				t.Errorf("panel %q target %s reads datasource uid %q, want %q",
					panel.Title, target.RefID, target.Datasource.UID, wantDatasourceUID)
			}
		}
	}
}

// TestAlertRulesMatchTheAcceptanceCriteria pins each alert's expression shape and,
// more importantly, its `for` duration: the durations are the thresholds that were
// argued for in the file's comments, and a silent change to one turns a considered
// alert into an arbitrary one.
func TestAlertRulesMatchTheAcceptanceCriteria(t *testing.T) {
	rules := decodeAlertRules(t)

	tests := []struct {
		alert     string
		wantFor   string
		wantExprs []string
	}{
		{
			alert:   "KubestreamWriteQueueSaturated",
			wantFor: "5m",
			wantExprs: []string{
				"kubestream_write_queue_depth / kubestream_write_queue_capacity > 0.8",
				// The guard against a capacity-0 sink making the ratio +Inf. It is
				// asserted because dropping it would leave an alert that fires on
				// every sink that has not published a capacity yet.
				"and kubestream_write_queue_capacity > 0",
			},
		},
		{
			alert:     "KubestreamWriteFailures",
			wantFor:   "10m",
			wantExprs: []string{`kubestream_writes_total{outcome="failed"}`, "> 0"},
		},
		{
			alert:     "KubestreamRuleNotReady",
			wantFor:   "15m",
			wantExprs: []string{`kubestream_rules{condition="Ready",status="false"} > 0`},
		},
		{
			alert:     "KubestreamEnqueueTimeouts",
			wantFor:   "5m",
			wantExprs: []string{"kubestream_enqueue_timeouts_total", "> 0"},
		},
	}

	byName := map[string]alertRule{}
	for _, r := range rules {
		byName[r.Alert] = r
	}
	if len(byName) != len(rules) {
		t.Errorf("alert names are not unique: %d rules, %d distinct names", len(rules), len(byName))
	}

	for _, tt := range tests {
		t.Run(tt.alert, func(t *testing.T) {
			rule, ok := byName[tt.alert]
			if !ok {
				t.Fatalf("alert %q is missing from %s", tt.alert, rel(alertsPath))
			}
			if rule.For != tt.wantFor {
				t.Errorf("alert %q fires after %q, want %q", tt.alert, rule.For, tt.wantFor)
			}
			if !containsAll(rule.Expr, tt.wantExprs) {
				t.Errorf("alert %q expression\n\t%s\ndoes not contain all of %q", tt.alert, rule.Expr, tt.wantExprs)
			}
		})
	}

	if len(rules) != len(tests) {
		t.Errorf("%s holds %d rules, want the %d the acceptance criteria name — a new rule needs a case here",
			rel(alertsPath), len(rules), len(tests))
	}
}

// TestQueriesOnlyUseExportedMetrics is the drift guard, and the reason this
// package imports the operator's own code.
//
// Every kubestream_* metric named by a dashboard panel or an alert is checked
// against the set the collectors *declare* — obtained from their Describe output,
// not from a Gather, so a metric whose series have no label values yet (safe_mode
// before the first warming scope) still counts as exported. A renamed or deleted
// metric fails here, at build time, instead of showing up as an empty panel.
func TestQueriesOnlyUseExportedMetrics(t *testing.T) {
	exported := declaredMetricNames(t)

	queries := map[string][]string{}
	for _, panel := range decodeDashboard(t).Panels {
		queries["panel "+panel.Title] = panel.exprs()
	}
	for _, rule := range decodeAlertRules(t) {
		queries["alert "+rule.Alert] = []string{rule.Expr}
	}

	sources := make([]string, 0, len(queries))
	for source := range queries {
		sources = append(sources, source)
	}
	sort.Strings(sources)

	for _, source := range sources {
		t.Run(source, func(t *testing.T) {
			for _, expr := range queries[source] {
				for _, referenced := range metricNamesIn(expr) {
					base := baseMetricName(referenced)
					if !slices.Contains(exported, base) {
						t.Errorf("%s queries %q, which the operator does not export.\nExported: %v",
							source, referenced, exported)
					}
				}
			}
		})
	}
}

// TestPromtoolChecksRules runs the real Prometheus rule checker over the
// PrometheusRule's .spec, which is the only way to know the PromQL parses.
//
// It resolves promtool from $PROMTOOL, then bin/, then $PATH, and skips when it
// finds none — `go test ./...` on a developer machine must not require a
// Prometheus build. CI does not skip: `make verify-observability` bootstraps
// promtool into bin/ and exports $PROMTOOL, and a $PROMTOOL that is set but
// unusable is a failure, not a skip.
func TestPromtoolChecksRules(t *testing.T) {
	promtool, fromEnv := findPromtool()
	if promtool == "" {
		t.Skip("promtool not found; run `make verify-observability` to include this check")
	}

	// promtool reads a plain rule file, so .spec is lifted out of the
	// PrometheusRule envelope. This is also the exact transformation an operator
	// running a non-operator-managed Prometheus performs by hand.
	var doc struct {
		Spec json.RawMessage `json:"spec"`
	}
	raw, err := os.ReadFile(alertsPath)
	if err != nil {
		t.Fatalf("read %s: %v", rel(alertsPath), err)
	}
	asJSON, err := yaml.YAMLToJSON(raw)
	if err != nil {
		t.Fatalf("convert %s to JSON: %v", rel(alertsPath), err)
	}
	if err := json.Unmarshal(asJSON, &doc); err != nil {
		t.Fatalf("decode %s: %v", rel(alertsPath), err)
	}
	specYAML, err := yaml.JSONToYAML(doc.Spec)
	if err != nil {
		t.Fatalf("render .spec as YAML: %v", err)
	}

	rulesFile := filepath.Join(t.TempDir(), "kubestream.rules.yaml")
	if err := os.WriteFile(rulesFile, specYAML, 0o600); err != nil {
		t.Fatalf("write the extracted rule file: %v", err)
	}

	out, err := exec.Command(promtool, "check", "rules", rulesFile).CombinedOutput()
	if err != nil {
		t.Fatalf("promtool check rules failed (%s, from env: %t):\n%s", promtool, fromEnv, out)
	}
	t.Logf("promtool check rules:\n%s", out)
}

func findPromtool() (path string, fromEnv bool) {
	if fromEnv := os.Getenv("PROMTOOL"); fromEnv != "" {
		return fromEnv, true
	}
	local := repoPath("bin", "promtool")
	if _, err := os.Stat(local); err == nil {
		return local, false
	}
	if found, err := exec.LookPath("promtool"); err == nil {
		return found, false
	}
	return "", false
}

// --- artifact decoding -------------------------------------------------------

type dashboard struct {
	UID    string  `json:"uid"`
	Title  string  `json:"title"`
	Panels []panel `json:"panels"`
}

type panel struct {
	ID         int           `json:"id"`
	Type       string        `json:"type"`
	Title      string        `json:"title"`
	Datasource datasourceRef `json:"datasource"`
	GridPos    gridPos       `json:"gridPos"`
	Targets    []target      `json:"targets"`
}

func (p panel) exprs() []string {
	out := make([]string, 0, len(p.Targets))
	for _, t := range p.Targets {
		out = append(out, t.Expr)
	}
	return out
}

type datasourceRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

type gridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

type target struct {
	RefID      string        `json:"refId"`
	Expr       string        `json:"expr"`
	Datasource datasourceRef `json:"datasource"`
}

type prometheusRule struct {
	Spec struct {
		Groups []struct {
			Name  string      `json:"name"`
			Rules []alertRule `json:"rules"`
		} `json:"groups"`
	} `json:"spec"`
}

type alertRule struct {
	Alert       string            `json:"alert"`
	Expr        string            `json:"expr"`
	For         string            `json:"for"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

func decodeDashboard(t *testing.T) dashboard {
	t.Helper()
	raw, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatalf("read %s: %v", rel(dashboardPath), err)
	}
	var dash dashboard
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("decode %s: %v", rel(dashboardPath), err)
	}
	return dash
}

func decodeAlertRules(t *testing.T) []alertRule {
	t.Helper()
	raw, err := os.ReadFile(alertsPath)
	if err != nil {
		t.Fatalf("read %s: %v", rel(alertsPath), err)
	}
	var doc prometheusRule
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode %s: %v", rel(alertsPath), err)
	}
	var rules []alertRule
	for _, group := range doc.Spec.Groups {
		rules = append(rules, group.Rules...)
	}
	return rules
}

func compileSchema(t *testing.T, path string) *jsonschema.Schema {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", rel(path), err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("%s is not valid JSON: %v", rel(path), err)
	}
	compiler := jsonschema.NewCompiler()
	name := filepath.Base(path)
	if err := compiler.AddResource(name, doc); err != nil {
		t.Fatalf("add %s to the compiler: %v", rel(path), err)
	}
	schema, err := compiler.Compile(name)
	if err != nil {
		t.Fatalf("compile %s: %v", rel(path), err)
	}
	return schema
}

func readDocument(t *testing.T, path string, isYAML bool) any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", rel(path), err)
	}
	if isYAML {
		converted, err := yaml.YAMLToJSON(raw)
		if err != nil {
			t.Fatalf("convert %s to JSON: %v", rel(path), err)
		}
		raw = converted
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("%s is not valid JSON: %v", rel(path), err)
	}
	return doc
}

// explain renders a validation failure as the detailed, per-location report.
//
// The default rendering is a single line naming only the outermost failure, which
// for a document as deeply nested as a dashboard says little more than "a panel is
// wrong somewhere".
func explain(err error) string {
	var verr *jsonschema.ValidationError
	if !errors.As(err, &verr) {
		return err.Error()
	}
	detailed, marshalErr := json.MarshalIndent(verr.DetailedOutput(), "", "  ")
	if marshalErr != nil {
		return err.Error()
	}
	return string(detailed)
}

// --- metric-name extraction --------------------------------------------------

// metricRef matches a kubestream metric name inside a PromQL expression. Only
// this operator's own metrics are extracted: a dashboard is free to reference
// something else (kube-state-metrics, say) and this suite has no standing to
// judge whether that exists.
var metricRef = regexp.MustCompile(`\bkubestream_[a-z0-9_]+\b`)

func metricNamesIn(expr string) []string {
	found := metricRef.FindAllString(expr, -1)
	slices.Sort(found)
	return slices.Compact(found)
}

// histogramSuffixes are the series a histogram publishes in addition to its
// declared name. Prometheus exposes kubestream_x_bucket for a histogram declared
// as kubestream_x, and a dashboard that did not query the bucket series could not
// compute a quantile at all.
var histogramSuffixes = []string{"_bucket", "_sum", "_count"}

func baseMetricName(name string) string {
	for _, suffix := range histogramSuffixes {
		// _total is deliberately not stripped: it is part of a counter's declared
		// name, not a generated suffix.
		if trimmed, ok := strings.CutSuffix(name, suffix); ok {
			return trimmed
		}
	}
	return name
}

// descFQName pulls a metric's fully-qualified name out of a Desc.
//
// prometheus.Desc exposes no accessor for it, and its String() form is the only
// route to the name — which is why this is a test-only concern and not something
// production code relies on.
var descFQName = regexp.MustCompile(`fqName: "([a-zA-Z_:][a-zA-Z0-9_:]*)"`)

// declaredMetricNames builds every collector the operator registers and returns
// the metric names they describe.
func declaredMetricNames(t *testing.T) []string {
	t.Helper()

	captured := &capturingRegisterer{}
	pipeline.NewPipelineMetrics(captured)
	controller.NewRuleMetrics(captured)
	if len(captured.collectors) == 0 {
		t.Fatal("no collectors were registered; the metric cross-check would pass vacuously")
	}

	var names []string
	for _, collector := range captured.collectors {
		descs := make(chan *prometheus.Desc)
		go func() {
			defer close(descs)
			collector.Describe(descs)
		}()
		for desc := range descs {
			match := descFQName.FindStringSubmatch(desc.String())
			if match == nil {
				t.Fatalf("cannot read a metric name out of %q; the Desc format changed", desc.String())
			}
			names = append(names, match[1])
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// capturingRegisterer collects the metrics handed to it instead of registering
// them, so the same constructors production calls can be inspected without
// standing up a registry (and without a second registration panicking).
type capturingRegisterer struct {
	collectors []prometheus.Collector
}

func (c *capturingRegisterer) Register(collector prometheus.Collector) error {
	c.collectors = append(c.collectors, collector)
	return nil
}

func (c *capturingRegisterer) MustRegister(collectors ...prometheus.Collector) {
	c.collectors = append(c.collectors, collectors...)
}

func (c *capturingRegisterer) Unregister(prometheus.Collector) bool { return false }

// --- small helpers -----------------------------------------------------------

func containsAll(haystack string, needles []string) bool {
	for _, needle := range needles {
		if !strings.Contains(haystack, needle) {
			return false
		}
	}
	return true
}

// rel renders a path relative to the repository root, so a failure message reads
// like the file an operator would open.
func rel(path string) string {
	root := repoPath()
	if trimmed, err := filepath.Rel(root, path); err == nil {
		return trimmed
	}
	return path
}
