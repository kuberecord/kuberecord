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
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	authzv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/plan"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/watch"
)

// Tests in this package run sequentially. No test here may call t.Parallel(), and
// TestNoTestInThisPackageRunsInParallel (sequential_test.go) fails the build if one
// does.
//
// This is a rule, not an observation about how the package happens to be written
// today. harness.stageRule below relaxes a rule CRD's schema for the duration of a
// single write and restores it immediately, and every test here shares one envtest
// apiserver — so a test running concurrently with that window can have its own object
// admitted against the relaxed schema, and the resulting failure names a file that
// has nothing to do with the cause.
//
// It is a boundary rather than an obstacle: a test that genuinely needs parallelism
// belongs in a package that does not use stageRule, where the shared relaxation
// window does not exist and t.Parallel() costs nothing.

// The control-plane reconcilers are almost entirely *about* the API server: what
// the REST mapper resolves, what a status subresource accepts, whether a field
// index and a map function actually re-enqueue what they claim to. Testing them
// against a fake client would assert the fake's behaviour, so this package runs
// against one envtest apiserver shared by every test — booting it dominates the
// runtime of these tests by orders of magnitude.
//
// Two dependencies stay fakes, deliberately. Access reviews are answered by
// fakeReviewer rather than by envtest RBAC, because the interesting scenario is a
// grant *appearing between two resyncs*, which a programmable fake makes an
// assertion instead of a race. And the sink runtime is fakeSinkRuntime, because
// the property under test is precisely that no reconcile path reaches a backend
// (Invariant 1) — a real ClickHouse would make that unfalsifiable.

var (
	// testEnv is the shared apiserver.
	testEnv *envtest.Environment

	// testCfg is its rest config, for tests that build their own clients.
	testCfg *rest.Config

	// testScheme carries the core types plus kuberecord.io/v1alpha1.
	testScheme = runtime.NewScheme()
)

func TestMain(m *testing.M) {
	os.Exit(runTestsWithEnvtest(m))
}

// runTestsWithEnvtest exists so envtest's teardown runs through a deferred call
// rather than racing os.Exit, which never runs defers.
func runTestsWithEnvtest(m *testing.M) (code int) {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(os.Stderr)))

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	if dir := firstEnvtestBinaryDir(); dir != "" {
		testEnv.BinaryAssetsDirectory = dir
	}

	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start envtest (run `make setup-envtest`): %v\n", err)
		return 1
	}
	defer func() {
		if stopErr := testEnv.Stop(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "failed to stop envtest: %v\n", stopErr)
			code = 1
		}
	}()
	testCfg = cfg

	if err := clientgoscheme.AddToScheme(testScheme); err != nil {
		fmt.Fprintf(os.Stderr, "failed to register the core scheme: %v\n", err)
		return 1
	}
	if err := v1alpha1.AddToScheme(testScheme); err != nil {
		fmt.Fprintf(os.Stderr, "failed to register kuberecord.io/v1alpha1: %v\n", err)
		return 1
	}

	return m.Run()
}

// firstEnvtestBinaryDir locates a downloaded envtest binary directory so these
// tests also run straight from an IDE, where KUBEBUILDER_ASSETS is not set by the
// Makefile. An empty result leaves envtest to its own lookup.
func firstEnvtestBinaryDir() string {
	entries, err := os.ReadDir(filepath.Join("..", "..", "bin", "k8s"))
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join("..", "..", "bin", "k8s", entry.Name())
		}
	}
	return ""
}

// harness is one test's fully wired control plane: a running manager with both rule
// reconcilers and the sink reconciler registered against the shared apiserver, plus
// the fakes whose answers the test manipulates.
//
// Each test gets its own manager (and therefore its own cache and its own registry)
// so that one test's rules can never appear in another's registry snapshot. They
// share the apiserver, so every test namespaces or uniquely names the objects it
// creates.
type harness struct {
	t *testing.T

	// Client is a direct (uncached) client, so a Get in an assertion never reads a
	// stale cache.
	Client client.Client

	// Registry is the desired-state registry the rule reconcilers project into —
	// the thing most assertions here are really about.
	Registry *plan.Registry

	// Reviewer decides access reviews. Tests flip its allow-set to simulate a grant
	// being added or removed.
	Reviewer *fakeReviewer

	// Runtime records what the sink reconciler declared, and never dials anything.
	Runtime *fakeSinkRuntime

	// Probes is the hub the reconcilers read health verdicts from; tests push
	// verdicts through ProbeResults. It is one hub for both sink kinds, exactly as
	// the composition root builds it, so these tests also cover the dispatch: a
	// verdict for an S3Sink must wake the S3 reconciler and no other.
	Probes *SinkProbeHub

	// ProbeResults is the channel a fake sink runtime's probes arrive on.
	ProbeResults chan sink.ProbeResult

	// Parker bridges "a sink is gone" back onto the rule reconcilers.
	Parker *Parker

	// RuleMetrics is the kuberecord_rules gauge both rule reconcilers of this
	// harness count into, on a registry private to the test. The process-wide
	// instance would work too, but its counts would then span every test in the
	// package running against the same apiserver.
	RuleMetrics *RuleMetrics
	metricsReg  *prometheus.Registry

	// OperatorNamespace is where this test's credentials Secrets live.
	OperatorNamespace string

	// cancel stops the manager; stopped reports when it has finished.
	cancel  context.CancelFunc
	stopped chan struct{}
}

// harnessOptions tunes a harness for one test.
type harnessOptions struct {
	// resyncPeriod overrides the reconcilers' resync. Tests that assert
	// self-healing shorten it drastically; everything else leaves it long so no
	// test depends on a resync it did not ask for.
	resyncPeriod time.Duration

	// allowAll makes every access review succeed. Tests that care about RBAC set
	// allowed instead.
	allowAll bool

	// allowed is the initial allow-set (resource plural names), used when allowAll
	// is false.
	allowed []string

	// buildConfig overrides the sink configuration builder. Nil means a builder
	// that fingerprints the spec and the password.
	buildConfig SinkConfigBuilder

	// s3BuildConfig overrides the S3 sink configuration builder. Nil means a
	// builder that fingerprints the spec and the access key.
	s3BuildConfig S3SinkConfigBuilder
}

// newHarness boots a manager with the three reconcilers wired up and returns once
// its cache has synced.
func newHarness(t *testing.T, opts harnessOptions) *harness {
	t.Helper()

	directClient, err := client.New(testCfg, client.Options{Scheme: testScheme})
	if err != nil {
		t.Fatalf("build a direct client: %v", err)
	}

	operatorNamespace := uniqueName("op")
	if err := directClient.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: operatorNamespace},
	}); err != nil {
		t.Fatalf("create the operator namespace: %v", err)
	}

	// Each test builds its own manager in one process, and controller-runtime's
	// controller-name registry is package-global (it exists to keep two controllers
	// from reporting the same metric series). Skipping that validation is a
	// test-harness concern only: production runs one manager per process, where the
	// check is exactly right.
	skipNameValidation := true
	mgr, err := ctrl.NewManager(testCfg, ctrl.Options{
		Scheme:     testScheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: ctrlconfig.Controller{SkipNameValidation: &skipNameValidation},
	})
	if err != nil {
		t.Fatalf("build a manager: %v", err)
	}

	h := &harness{
		t:                 t,
		Client:            directClient,
		Registry:          plan.New(),
		Reviewer:          newFakeReviewer(opts.allowAll, opts.allowed...),
		Runtime:           newFakeSinkRuntime(),
		ProbeResults:      make(chan sink.ProbeResult, 16),
		OperatorNamespace: operatorNamespace,
		stopped:           make(chan struct{}),
		metricsReg:        prometheus.NewRegistry(),
	}
	h.RuleMetrics = NewRuleMetrics(h.metricsReg)

	h.Probes = NewSinkProbeHub(h.ProbeResults)
	if err := h.Probes.SetupWithManager(mgr); err != nil {
		t.Fatalf("set up the sink probe hub: %v", err)
	}

	buildConfig := opts.buildConfig
	if buildConfig == nil {
		buildConfig = fakeConfigBuilder
	}
	s3BuildConfig := opts.s3BuildConfig
	if s3BuildConfig == nil {
		s3BuildConfig = fakeS3ConfigBuilder
	}
	sinkReconciler := &SinkReconciler{
		Client: mgr.GetClient(),
		//nolint:staticcheck // SA1019: matches the composition root; see cmd/main.go.
		Recorder:          mgr.GetEventRecorderFor("kuberecord-test"),
		Sinks:             h.Runtime,
		BuildConfig:       buildConfig,
		OperatorNamespace: operatorNamespace,
		Probes:            h.Probes,
		ResyncPeriod:      opts.resyncPeriod,
	}
	if err := sinkReconciler.SetupWithManager(mgr); err != nil {
		t.Fatalf("set up the sink reconciler: %v", err)
	}

	s3SinkReconciler := &S3SinkReconciler{
		Client: mgr.GetClient(),
		//nolint:staticcheck // SA1019: matches the composition root; see cmd/main.go.
		Recorder:          mgr.GetEventRecorderFor("kuberecord-test"),
		Sinks:             h.Runtime,
		BuildConfig:       s3BuildConfig,
		OperatorNamespace: operatorNamespace,
		Probes:            h.Probes,
		ResyncPeriod:      opts.resyncPeriod,
	}
	if err := s3SinkReconciler.SetupWithManager(mgr); err != nil {
		t.Fatalf("set up the S3 sink reconciler: %v", err)
	}

	base := RuleReconciler{
		Client: mgr.GetClient(),
		//nolint:staticcheck // SA1019: matches the composition root; see cmd/main.go.
		Recorder:     mgr.GetEventRecorderFor("kuberecord-test"),
		Registry:     h.Registry,
		Resolver:     watch.NewResolver(mgr.GetRESTMapper()),
		Access:       h.Reviewer,
		ResyncPeriod: opts.resyncPeriod,
		Metrics:      h.RuleMetrics,
	}
	namespaced := NewStreamRuleReconciler(base)
	if err := namespaced.SetupWithManager(mgr); err != nil {
		t.Fatalf("set up the StreamRule reconciler: %v", err)
	}
	clusterWide := NewClusterStreamRuleReconciler(base)
	if err := clusterWide.SetupWithManager(mgr); err != nil {
		t.Fatalf("set up the ClusterStreamRule reconciler: %v", err)
	}
	h.Parker = NewParker(namespaced, clusterWide)

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		defer close(h.stopped)
		if err := mgr.Start(ctx); err != nil {
			// Reported rather than fataled: t.Fatalf from a non-test goroutine is
			// undefined, and a manager that failed to start shows up as every
			// subsequent assertion timing out anyway.
			t.Errorf("manager stopped with an error: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatalf("the manager's cache never synced")
	}

	t.Cleanup(h.stop)
	return h
}

// stop shuts the manager down and waits for it, so no test's goroutines outlive it
// (and no leaked reconcile writes into another test's objects).
func (h *harness) stop() {
	h.cancel()
	select {
	case <-h.stopped:
	case <-time.After(30 * time.Second):
		h.t.Error("the manager did not stop within 30s")
	}
}

// pushProbe delivers one probe verdict as the sink runtime would. It returns once
// the watcher has taken it off the channel; the caller then waits on the condition
// the verdict produces, which is the observable effect worth synchronising on.
func (h *harness) pushProbe(result sink.ProbeResult) {
	h.t.Helper()
	select {
	case h.ProbeResults <- result:
	case <-time.After(5 * time.Second):
		h.t.Fatalf("the probe hub never drained a result for sink %q", result.Sink)
	}
}

// createNamespace creates a labelled namespace and registers its deletion.
func (h *harness) createNamespace(name string, labels map[string]string) {
	h.t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	if err := h.Client.Create(context.Background(), ns); err != nil {
		h.t.Fatalf("create namespace %q: %v", name, err)
	}
	// Namespaces are not deleted on cleanup: envtest has no namespace controller,
	// so a Delete leaves them Terminating forever and a later test reusing the name
	// would fail. Names are unique per test instead.
}

// labelNamespace adds or removes a label on a namespace.
func (h *harness) labelNamespace(name string, labels map[string]string) {
	h.t.Helper()
	var ns corev1.Namespace
	if err := h.Client.Get(context.Background(), client.ObjectKey{Name: name}, &ns); err != nil {
		h.t.Fatalf("get namespace %q: %v", name, err)
	}
	ns.Labels = labels
	if err := h.Client.Update(context.Background(), &ns); err != nil {
		h.t.Fatalf("relabel namespace %q: %v", name, err)
	}
}

// createSecret creates a credentials Secret in the operator namespace.
func (h *harness) createSecret(name, password string) {
	h.t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.OperatorNamespace},
		Data:       map[string][]byte{DefaultCredentialsSecretKey: []byte(password)},
	}
	if err := h.Client.Create(context.Background(), secret); err != nil {
		h.t.Fatalf("create secret %q: %v", name, err)
	}
}

// createReadySink creates a ClickHouseSink and drives it to Ready=True by pushing a
// successful probe, so rule tests can start from a healthy sink without repeating
// the sink handshake.
func (h *harness) createReadySink(name string, policy v1alpha1.SinkPolicy) {
	h.t.Helper()
	h.createSecret(name, "s3cret")
	chSink := &v1alpha1.ClickHouseSink{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.ClickHouseSinkSpec{
			Connection: v1alpha1.ConnectionSpec{
				Addr:                 "clickhouse.invalid:9000",
				CredentialsSecretRef: v1alpha1.SecretReference{Name: name},
			},
			Policy: policy,
		},
	}
	if err := h.Client.Create(context.Background(), chSink); err != nil {
		h.t.Fatalf("create sink %q: %v", name, err)
	}
	h.t.Cleanup(func() { h.deleteIfExists(chSink) })

	// The first reconcile resolves the credential and declares the sink; the probe
	// is what flips SchemaValid and therefore Ready.
	h.waitForSinkCondition(name, v1alpha1.ConditionCredentialsResolved, metav1.ConditionTrue, ReasonSecretResolved)
	h.pushProbe(sink.ProbeResult{Sink: clickHouseSinkID(name), At: time.Now().UTC()})
	h.waitForSinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonConnected)
}

// deleteIfExists removes a test object, ignoring an already-deleted one.
func (h *harness) deleteIfExists(obj client.Object) {
	if err := h.Client.Delete(context.Background(), obj); err != nil && !apierrors.IsNotFound(err) {
		h.t.Errorf("cleanup: deleting %s failed: %v", obj.GetName(), err)
	}
}

// conditionAbsent is what a condition poll reports while the condition it is
// waiting for has not been written at all. It is one constant so a timeout message
// reads the same whichever object the poll was watching.
const conditionAbsent = "condition absent"

// waitTimeout bounds every eventual assertion. It is generous because envtest's
// apiserver shares a machine with the rest of the suite; a genuine failure still
// fails, just later.
const waitTimeout = 20 * time.Second

// waitFor polls cond until it holds, failing the test with describe's last value on
// timeout. Polling (rather than watching) keeps the assertions readable and is what
// every controller test in this repo's ecosystem does.
func waitFor(t *testing.T, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	var last string
	for time.Now().Before(deadline) {
		ok, describe := cond()
		if ok {
			return
		}
		last = describe
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; last observed: %s", what, last)
}

// waitForSinkCondition waits until a sink carries the given condition status and
// reason.
func (h *harness) waitForSinkCondition(name, condType string, want metav1.ConditionStatus, reason string) {
	h.t.Helper()
	waitFor(h.t, fmt.Sprintf("sink %q condition %s=%s/%s", name, condType, want, reason), func() (bool, string) {
		var chSink v1alpha1.ClickHouseSink
		if err := h.Client.Get(context.Background(), client.ObjectKey{Name: name}, &chSink); err != nil {
			return false, err.Error()
		}
		c := findCondition(chSink.Status.Conditions, condType)
		if c == nil {
			return false, conditionAbsent
		}
		return c.Status == want && c.Reason == reason, fmt.Sprintf("%s/%s: %s", c.Status, c.Reason, c.Message)
	})
}

// waitForRuleCondition waits until a rule carries the given condition status and
// reason, and returns the condition it settled on so a test can assert on its
// message.
func (h *harness) waitForRuleCondition(obj client.Object, condType string,
	want metav1.ConditionStatus, reason string) metav1.Condition {
	h.t.Helper()
	var settled metav1.Condition
	key := client.ObjectKeyFromObject(obj)
	waitFor(h.t, fmt.Sprintf("rule %s condition %s=%s/%s", key, condType, want, reason), func() (bool, string) {
		fresh, ok := obj.DeepCopyObject().(client.Object)
		if !ok {
			return false, "not a client.Object"
		}
		if err := h.Client.Get(context.Background(), key, fresh); err != nil {
			return false, err.Error()
		}
		conditions := ruleConditions(fresh)
		c := findCondition(conditions, condType)
		if c == nil {
			return false, conditionAbsent
		}
		if c.Status == want && c.Reason == reason {
			settled = *c
			return true, ""
		}
		return false, fmt.Sprintf("%s/%s: %s", c.Status, c.Reason, c.Message)
	})
	return settled
}

// ruleConditions reads either rule type's conditions.
func ruleConditions(obj client.Object) []metav1.Condition {
	switch typed := obj.(type) {
	case *v1alpha1.StreamRule:
		return typed.Status.Conditions
	case *v1alpha1.ClusterStreamRule:
		return typed.Status.Conditions
	default:
		return nil
	}
}

// waitForTargets waits until the rule's registry contribution is exactly want,
// compared as a sorted set of "gvk@namespace" strings so the assertion reads like
// the intent rather than like a map literal.
func (h *harness) waitForTargets(ruleKey string, want []string) {
	h.t.Helper()
	waitFor(h.t, fmt.Sprintf("rule %q targets %v", ruleKey, want), func() (bool, string) {
		got := h.targetsFor(ruleKey)
		return slicesEqual(got, want), fmt.Sprintf("%v", got)
	})
}

// waitForReadyGauge blocks until kuberecord_rules{condition="Ready",status} reaches
// want. It polls rather than reading once because the gauge is published by a
// reconcile that has to happen first, exactly like every other assertion here.
//
// The roll-up condition is the only one asserted through the harness: it is the
// one every rule carries and the one the shipped alert is written against. The
// per-condition arithmetic is covered against a bare RuleMetrics in
// metrics_test.go, where it needs no apiserver.
func (h *harness) waitForReadyGauge(status string, want float64) {
	h.t.Helper()
	series := fmt.Sprintf("kuberecord_rules{condition=%q,status=%q}", v1alpha1.ConditionReady, status)
	waitFor(h.t, fmt.Sprintf("%s = %v", series, want), func() (bool, string) {
		got := testutil.ToFloat64(h.RuleMetrics.rules.WithLabelValues(v1alpha1.ConditionReady, status))
		return got == want, fmt.Sprintf("%v", got)
	})
}

// targetsFor renders the registry's current targets for one rule, sorted.
func (h *harness) targetsFor(ruleKey string) []string {
	var got []string
	for key, state := range h.Registry.Snapshot() {
		for _, rule := range state.RuleKeys {
			if rule != ruleKey {
				continue
			}
			got = append(got, fmt.Sprintf("%s@%s", key.GVK, key.Namespace))
		}
	}
	slices.Sort(got)
	return got
}

// targetSinksFor returns the distinct sink identities one rule's installed targets
// stream to, ordered by ID.Compare.
//
// It is a separate accessor from targetsFor rather than another field in its
// rendering, because the two answer different questions: targetsFor asks "which
// scopes is this rule watching?", which every rule test cares about, and this asks
// "which backend do those scopes write to?", which only the typed-identity tests do.
func (h *harness) targetSinksFor(ruleKey string) []sink.ID {
	seen := make(map[sink.ID]struct{})
	for key, state := range h.Registry.Snapshot() {
		for _, rule := range state.RuleKeys {
			if rule == ruleKey {
				seen[key.Sink] = struct{}{}
			}
		}
	}
	ids := slices.Collect(maps.Keys(seen))
	slices.SortFunc(ids, sink.ID.Compare)
	return ids
}

// ruleCRDNames maps a rule kind discriminator onto the CRD that serves it, for the
// staging helpers below.
var ruleCRDNames = map[string]string{
	kindStreamRule:        "streamrules.kuberecord.io",
	kindClusterStreamRule: "clusterstreamrules.kuberecord.io",
}

// crdGVK identifies a CustomResourceDefinition for the unstructured client below.
//
// The CRD is read and written as an unstructured object on purpose: importing
// apiextensions-apiserver for its typed API would promote an indirect dependency to
// a direct one, and this needs two field edits in a schema, not an API.
var crdGVK = schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}

// stageRule stores a rule the API server would refuse to admit today, by relaxing
// its CRD for exactly as long as the write takes.
//
// It exists because the two guards this file's tests exercise are guards on objects
// that are *already in etcd*: a rule written against v0.1.0's spec.sinkRef, and a
// rule naming a sink kind this build does not serve. Admission rejects both — the
// required field and the kind enum are Task 4.3's job and they work — so writing the
// object under a schema that admitted it and then restoring the shipped one is not a
// trick to get around validation, it *is* the situation being tested: exactly what an
// operator upgrade leaves behind.
//
// relax is handed the CRD's `spec` schema (properties.spec) to edit in place. The
// shipped CRD is restored before this returns, so every reconcile the test then
// observes runs against precisely the schema production runs — including the status
// writes onto the staged object, which is the property that makes a condition on an
// invalid object a real signal rather than a test artefact.
//
// The window in which the schema is relaxed cannot admit another test's object
// because this package runs sequentially — see the package comment at the top of
// this file, which states that rule and names the test that enforces it.
func (h *harness) stageRule(ruleKind, namespace, name string,
	spec map[string]any, relax func(specSchema map[string]any) error) client.Object {
	h.t.Helper()
	ctx := context.Background()
	crdName, known := ruleCRDNames[ruleKind]
	if !known {
		h.t.Fatalf("no CRD is registered for rule kind %q", ruleKind)
	}

	crd := h.getCRD(crdName)
	// The whole spec is kept, not just the field being relaxed: restoring by
	// replaying an inverse edit would quietly leave the schema wrong if the edit and
	// its inverse ever disagreed.
	original, ok := runtime.DeepCopyJSONValue(crd.Object["spec"]).(map[string]any)
	if !ok {
		h.t.Fatalf("CRD %s has no spec object", crdName)
	}
	if err := relax(h.ruleSpecSchema(crd, crdName)); err != nil {
		h.t.Fatalf("relax the %s schema: %v", crdName, err)
	}
	if err := h.Client.Update(ctx, crd); err != nil {
		h.t.Fatalf("relax CRD %s: %v", crdName, err)
	}
	defer h.restoreCRD(crdName, original)

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       ruleKindToCRKind[ruleKind],
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"spec":       spec,
	}}
	// Polled rather than attempted once: a CRD schema update reaches the apiserver's
	// request path through its own informer, so the first write after the relaxation
	// can still be judged by the strict schema.
	waitFor(h.t, fmt.Sprintf("the relaxed %s schema to admit %s/%s", crdName, namespace, name),
		func() (bool, string) {
			err := h.Client.Create(ctx, obj)
			if err == nil {
				return true, ""
			}
			return false, err.Error()
		})

	stub := newRuleStub(ruleKind, types.NamespacedName{Namespace: namespace, Name: name})
	h.t.Cleanup(func() { h.deleteIfExists(stub) })
	return stub
}

// ruleKindToCRKind maps a rule kind discriminator onto the Kind an API object of it
// carries, which is what an unstructured write has to spell.
var ruleKindToCRKind = map[string]string{
	kindStreamRule:        "StreamRule",
	kindClusterStreamRule: "ClusterStreamRule",
}

// getCRD reads one CRD as an unstructured object.
func (h *harness) getCRD(name string) *unstructured.Unstructured {
	h.t.Helper()
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(crdGVK)
	if err := h.Client.Get(context.Background(), client.ObjectKey{Name: name}, crd); err != nil {
		h.t.Fatalf("get CRD %s: %v", name, err)
	}
	return crd
}

// ruleSpecSchema returns the `spec` schema of a rule CRD's single served version,
// for in-place editing. NestedFieldNoCopy is what makes the edit land on the object
// that is about to be sent, rather than on a copy of it.
func (h *harness) ruleSpecSchema(crd *unstructured.Unstructured, crdName string) map[string]any {
	h.t.Helper()
	raw, found, err := unstructured.NestedFieldNoCopy(crd.Object, "spec", "versions")
	if err != nil || !found {
		h.t.Fatalf("CRD %s has no versions: %v", crdName, err)
	}
	list, ok := raw.([]any)
	if !ok || len(list) != 1 {
		// One served version is not incidental: v1alpha1 is the only version these
		// CRDs have, and a second one would mean this helper had to pick.
		h.t.Fatalf("CRD %s does not serve exactly one version: %#v", crdName, raw)
	}
	version, ok := list[0].(map[string]any)
	if !ok {
		h.t.Fatalf("CRD %s version 0 is not an object", crdName)
	}
	specSchema, found, err := unstructured.NestedFieldNoCopy(version,
		"schema", "openAPIV3Schema", "properties", "spec")
	if err != nil || !found {
		h.t.Fatalf("CRD %s has no spec schema: %v", crdName, err)
	}
	typed, ok := specSchema.(map[string]any)
	if !ok {
		h.t.Fatalf("CRD %s spec schema is not an object", crdName)
	}
	return typed
}

// restoreCRD puts the shipped schema back, retrying the conflict that the
// apiextensions controllers' own status writes make routine.
func (h *harness) restoreCRD(name string, spec map[string]any) {
	h.t.Helper()
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(crdGVK)
		if getErr := h.Client.Get(context.Background(), client.ObjectKey{Name: name}, crd); getErr != nil {
			return getErr
		}
		crd.Object["spec"] = runtime.DeepCopyJSONValue(spec)
		return h.Client.Update(context.Background(), crd)
	})
	if err != nil {
		// Fatal rather than reported: a CRD left relaxed would silently weaken every
		// admission assertion made after this point in the package.
		h.t.Fatalf("restore CRD %s: %v", name, err)
	}
}

// dropRequiredSink removes `sink` from a rule spec's required fields, which is what
// makes a v0.1.0-shaped rule storable.
func dropRequiredSink(specSchema map[string]any) error {
	required, found, err := unstructured.NestedStringSlice(specSchema, "required")
	if err != nil || !found {
		return fmt.Errorf("read the spec's required fields (found=%v): %w", found, err)
	}
	kept := slices.DeleteFunc(required, func(field string) bool { return field == "sink" })
	if len(kept) == len(required) {
		return fmt.Errorf("required = %v, expected it to list \"sink\"", required)
	}
	return unstructured.SetNestedStringSlice(specSchema, kept, "required")
}

// dropSinkKindEnum removes the enum from spec.sink.kind, which is what makes a rule
// naming a sink kind this build does not serve storable.
func dropSinkKindEnum(specSchema map[string]any) error {
	kindSchema, found, err := unstructured.NestedFieldNoCopy(specSchema, "properties", "sink", "properties", "kind")
	if err != nil || !found {
		return fmt.Errorf("read the sink kind schema (found=%v): %w", found, err)
	}
	typed, ok := kindSchema.(map[string]any)
	if !ok {
		return fmt.Errorf("the sink kind schema is %T, not an object", kindSchema)
	}
	if _, enumerated := typed["enum"]; !enumerated {
		return fmt.Errorf("spec.sink.kind carries no enum to relax")
	}
	delete(typed, "enum")
	return nil
}

// unstructuredResources renders a rule's resource list for an unstructured write.
// Label selectors are not rendered: no staged rule needs one, and the selector path
// is covered by the rules the tests create through the typed client.
func unstructuredResources(resources []v1alpha1.WatchedResource) []any {
	out := make([]any, 0, len(resources))
	for _, res := range resources {
		out = append(out, map[string]any{"group": res.Group, "version": res.Version, "kind": res.Kind})
	}
	return out
}

// uniqueName builds a DNS-1123 name unique within one apiserver's lifetime, so
// tests sharing the apiserver never collide.
func uniqueName(prefix string) string {
	nameCounterMu.Lock()
	defer nameCounterMu.Unlock()
	nameCounter++
	return fmt.Sprintf("%s-%d", strings.ToLower(prefix), nameCounter)
}

var (
	nameCounterMu sync.Mutex
	nameCounter   int
)

// coreGVK renders a core v1 GroupVersionKind the way waitForTargets spells one. Only
// core v1 kinds are needed here: the envtest apiserver serves them without any CRD, so
// a resolution assertion never depends on a fixture, and the group/version handling
// itself is covered without an apiserver in TestCheckPolicy.
func coreGVK(kind string) string {
	return schema.GroupVersionKind{Version: "v1", Kind: kind}.String()
}

// fakeReviewer answers SelfSubjectAccessReviews from an allow-set of plural
// resource names, so a test can grant or revoke access between two reconciles.
//
// It records every question asked, which is how the SSAR-count assertions (the
// cluster-scope short-circuit) are made.
type fakeReviewer struct {
	mu       sync.Mutex
	allowAll bool
	// allowed holds plural resource names that are allowed at every scope.
	allowed map[string]struct{}
	// asked records "resource|namespace|verb" for every review.
	asked []string
	// err, when set, fails every review — the "the review itself broke" case.
	err error
}

func newFakeReviewer(allowAll bool, allowed ...string) *fakeReviewer {
	f := &fakeReviewer{allowAll: allowAll, allowed: make(map[string]struct{}, len(allowed))}
	for _, resource := range allowed {
		f.allowed[resource] = struct{}{}
	}
	return f
}

// allow adds resources to the allow-set, as an administrator adding a grant would.
func (f *fakeReviewer) allow(resources ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, resource := range resources {
		f.allowed[resource] = struct{}{}
	}
}

// fail makes every subsequent review return err.
func (f *fakeReviewer) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// questions returns the reviews asked so far.
func (f *fakeReviewer) questions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...)
}

func (f *fakeReviewer) Create(_ context.Context, review *authzv1.SelfSubjectAccessReview,
	_ metav1.CreateOptions) (*authzv1.SelfSubjectAccessReview, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	attrs := review.Spec.ResourceAttributes
	f.asked = append(f.asked, fmt.Sprintf("%s|%s|%s", attrs.Resource, attrs.Namespace, attrs.Verb))

	out := review.DeepCopy()
	_, allowed := f.allowed[attrs.Resource]
	out.Status.Allowed = f.allowAll || allowed
	return out, nil
}

// fakeSinkRuntime records the configurations the sink reconciler declared and the
// sinks it withdrew. It never builds anything and never dials anything, which is
// the point: the reconciler's whole interaction with a backend is a struct
// hand-off.
//
// It is keyed by sink.ID, as the real runtime is, so these tests also assert that
// the reconciler declares its sinks under the ClickHouseSink kind rather than
// under a bare name (Task 4.1): a wrong kind would simply not be found by the
// accessors below.
type fakeSinkRuntime struct {
	mu       sync.Mutex
	ensured  map[sink.ID][]string // sink → fingerprints, in order
	deleted  []sink.ID
	ensueErr error
}

func newFakeSinkRuntime() *fakeSinkRuntime {
	return &fakeSinkRuntime{ensured: make(map[sink.ID][]string)}
}

func (f *fakeSinkRuntime) Ensure(id sink.ID, cfg sink.InstanceConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ensueErr != nil {
		return f.ensueErr
	}
	fingerprints := f.ensured[id]
	fingerprint := cfg.Fingerprint()
	// Only a *change* is recorded, so a test asserting "the password rotation
	// recycled the instance" is asserting the same thing the production runtime
	// would act on rather than counting reconciles.
	if len(fingerprints) == 0 || fingerprints[len(fingerprints)-1] != fingerprint {
		f.ensured[id] = append(fingerprints, fingerprint)
	}
	return nil
}

func (f *fakeSinkRuntime) Delete(id sink.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, id)
}

// fingerprints returns the distinct configurations declared for one ClickHouseSink,
// in order. It takes a name and resolves the ID itself, which is also what makes it
// an assertion about the *kind*: a reconciler declaring its sink under a bare name,
// or under the wrong kind, would simply not be found here.
func (f *fakeSinkRuntime) fingerprints(name string) []string {
	return f.fingerprintsFor(clickHouseSinkID(name))
}

// s3Fingerprints is the same accessor for an S3Sink, and exists separately for the
// same reason: it names the kind, so a sink declared under the wrong one is a
// failing assertion rather than a passing one about a different backend.
func (f *fakeSinkRuntime) s3Fingerprints(name string) []string {
	return f.fingerprintsFor(s3SinkID(name))
}

func (f *fakeSinkRuntime) fingerprintsFor(id sink.ID) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ensured[id]...)
}

// deletions returns the sinks withdrawn so far.
func (f *fakeSinkRuntime) deletions() []sink.ID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sink.ID(nil), f.deleted...)
}

// fakeConfig is a minimal sink.InstanceConfig: a fingerprint and nothing else, since
// nothing in the control plane reads a configuration back.
type fakeConfig struct{ fingerprint string }

func (c fakeConfig) Fingerprint() string { return c.fingerprint }

// fakeConfigBuilder fingerprints the fields a real backend configuration would be
// built from, so a rotated password changes the fingerprint exactly as
// clickhouse.Config.Fingerprint would.
func fakeConfigBuilder(name string, spec v1alpha1.ClickHouseSinkSpec, password string) (sink.InstanceConfig, error) {
	return fakeConfig{fingerprint: fmt.Sprintf("%s|%s|%s|%s",
		name, spec.Connection.Addr, spec.Connection.Username, password)}, nil
}

// fakeS3ConfigBuilder fingerprints the fields a real object-store configuration
// would be built from, so a rotated access key changes the fingerprint exactly as
// s3.SinkConfig.Fingerprint would — including the ambient case, where the key is
// empty and the fingerprint must still cover the rest of the spec.
func fakeS3ConfigBuilder(name string, spec v1alpha1.S3SinkSpec,
	creds S3Credentials) (sink.InstanceConfig, error) {
	return fakeConfig{fingerprint: fmt.Sprintf("%s|%s|%s|%s|%t|%s|%s",
		name, spec.Bucket, spec.Prefix, spec.Endpoint, spec.ForcePathStyle,
		creds.AccessKeyID, creds.SecretAccessKey)}, nil
}

// createS3Secret creates an S3 credentials Secret in the operator namespace.
func (h *harness) createS3Secret(name, accessKeyID, secretAccessKey string) {
	h.t.Helper()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: h.OperatorNamespace},
		Data: map[string][]byte{
			DefaultAccessKeyIDSecretKey:     []byte(accessKeyID),
			DefaultSecretAccessKeySecretKey: []byte(secretAccessKey),
		},
	}
	if err := h.Client.Create(context.Background(), secret); err != nil {
		h.t.Fatalf("create S3 secret %q: %v", name, err)
	}
}

// createS3Sink creates an S3Sink naming a Secret of the same name, and registers its
// deletion. It does not wait for any condition — the tests that call it are about
// how those conditions settle — and returns nothing: a sink is read back through
// waitForS3SinkCondition or the API, never from a stale copy of what was created.
func (h *harness) createS3Sink(name string, mutate func(*v1alpha1.S3Sink)) {
	h.t.Helper()
	s3Sink := &v1alpha1.S3Sink{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.S3SinkSpec{
			Bucket:      "kuberecord-audit",
			Credentials: &v1alpha1.S3CredentialsSpec{SecretRef: &v1alpha1.SecretReference{Name: name}},
		},
	}
	if mutate != nil {
		mutate(s3Sink)
	}
	if err := h.Client.Create(context.Background(), s3Sink); err != nil {
		h.t.Fatalf("create S3 sink %q: %v", name, err)
	}
	h.t.Cleanup(func() { h.deleteIfExists(s3Sink) })
}

// createReadyS3Sink creates an S3Sink with its Secret and drives it to Ready=True
// by pushing a successful probe, so a rule test can start from a healthy archive
// sink without repeating the handshake.
func (h *harness) createReadyS3Sink(name string, policy v1alpha1.SinkPolicy) {
	h.t.Helper()
	// The Secret is named after the sink *and its kind*, because a test may create a
	// ClickHouseSink and an S3Sink under one name — that collision is the whole
	// subject of the typed-identity tests — and the two would otherwise fight over
	// one Secret object.
	secretName := name + "-s3"
	h.createS3Secret(secretName, "AKIATEST", "secret-access-key")
	h.createS3Sink(name, func(s *v1alpha1.S3Sink) {
		s.Spec.Policy = policy
		s.Spec.Credentials = &v1alpha1.S3CredentialsSpec{
			SecretRef: &v1alpha1.SecretReference{Name: secretName},
		}
	})

	h.waitForS3SinkCondition(name, v1alpha1.ConditionCredentialsResolved,
		metav1.ConditionTrue, ReasonSecretResolved)
	h.pushProbe(sink.ProbeResult{Sink: s3SinkID(name), At: time.Now().UTC()})
	h.waitForS3SinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonArchiving)
}

// waitForS3SinkCondition waits until an S3Sink carries the given condition status
// and reason, and returns the condition it settled on so a test can assert on its
// message.
func (h *harness) waitForS3SinkCondition(name, condType string,
	want metav1.ConditionStatus, reason string) metav1.Condition {
	h.t.Helper()
	var settled metav1.Condition
	waitFor(h.t, fmt.Sprintf("S3 sink %q condition %s=%s/%s", name, condType, want, reason),
		func() (bool, string) {
			var s3Sink v1alpha1.S3Sink
			if err := h.Client.Get(context.Background(), client.ObjectKey{Name: name}, &s3Sink); err != nil {
				return false, err.Error()
			}
			c := findCondition(s3Sink.Status.Conditions, condType)
			if c == nil {
				return false, conditionAbsent
			}
			if c.Status == want && c.Reason == reason {
				settled = *c
				return true, ""
			}
			return false, fmt.Sprintf("%s/%s: %s", c.Status, c.Reason, c.Message)
		})
	return settled
}

// slicesEqual compares two string slices element-wise, treating nil and empty as
// equal so an assertion can spell "no targets" as nil.
func slicesEqual(a, b []string) bool {
	return slices.Equal(a, b)
}
