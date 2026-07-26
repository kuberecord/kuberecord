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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	authzv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/yelzhy/kubestream/api/v1alpha1"
	"github.com/yelzhy/kubestream/internal/plan"
	"github.com/yelzhy/kubestream/internal/sink"
	"github.com/yelzhy/kubestream/internal/watch"
)

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

	// testScheme carries the core types plus kubestream.io/v1alpha1.
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
		fmt.Fprintf(os.Stderr, "failed to register kubestream.io/v1alpha1: %v\n", err)
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

	// Probes is the store the reconciler reads health verdicts from; tests push
	// verdicts through ProbeResults.
	Probes *probeStore

	// ProbeResults is the channel a fake sink runtime's probes arrive on.
	ProbeResults chan sink.ProbeResult

	// Parker bridges "a sink is gone" back onto the rule reconcilers.
	Parker *Parker

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
		Probes:            newProbeStore(),
		ProbeResults:      make(chan sink.ProbeResult, 16),
		OperatorNamespace: operatorNamespace,
		stopped:           make(chan struct{}),
	}

	buildConfig := opts.buildConfig
	if buildConfig == nil {
		buildConfig = fakeConfigBuilder
	}
	sinkReconciler := &SinkReconciler{
		Client:            mgr.GetClient(),
		Recorder:          mgr.GetEventRecorderFor("kubestream-test"),
		Sinks:             h.Runtime,
		BuildConfig:       buildConfig,
		OperatorNamespace: operatorNamespace,
		Probes:            h.Probes,
		ResyncPeriod:      opts.resyncPeriod,
	}
	if err := sinkReconciler.SetupWithManager(mgr, h.ProbeResults); err != nil {
		t.Fatalf("set up the sink reconciler: %v", err)
	}

	base := RuleReconciler{
		Client:       mgr.GetClient(),
		Recorder:     mgr.GetEventRecorderFor("kubestream-test"),
		Registry:     h.Registry,
		Resolver:     watch.NewResolver(mgr.GetRESTMapper()),
		Access:       h.Reviewer,
		ResyncPeriod: opts.resyncPeriod,
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
		h.t.Fatalf("the probe watcher never drained a result for sink %q", result.Sink)
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
	h.pushProbe(sink.ProbeResult{Sink: name, At: time.Now().UTC()})
	h.waitForSinkCondition(name, v1alpha1.ConditionReady, metav1.ConditionTrue, ReasonConnected)
}

// deleteIfExists removes a test object, ignoring an already-deleted one.
func (h *harness) deleteIfExists(obj client.Object) {
	if err := h.Client.Delete(context.Background(), obj); err != nil && !apierrors.IsNotFound(err) {
		h.t.Errorf("cleanup: deleting %s failed: %v", obj.GetName(), err)
	}
}

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
			return false, "condition absent"
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
			return false, "condition absent"
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
// names it withdrew. It never builds anything and never dials anything, which is
// the point: the reconciler's whole interaction with a backend is a struct
// hand-off.
type fakeSinkRuntime struct {
	mu       sync.Mutex
	ensured  map[string][]string // sink name → fingerprints, in order
	deleted  []string
	ensueErr error
}

func newFakeSinkRuntime() *fakeSinkRuntime {
	return &fakeSinkRuntime{ensured: make(map[string][]string)}
}

func (f *fakeSinkRuntime) Ensure(name string, cfg sink.InstanceConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ensueErr != nil {
		return f.ensueErr
	}
	fingerprints := f.ensured[name]
	fingerprint := cfg.Fingerprint()
	// Only a *change* is recorded, so a test asserting "the password rotation
	// recycled the instance" is asserting the same thing the production runtime
	// would act on rather than counting reconciles.
	if len(fingerprints) == 0 || fingerprints[len(fingerprints)-1] != fingerprint {
		f.ensured[name] = append(fingerprints, fingerprint)
	}
	return nil
}

func (f *fakeSinkRuntime) Delete(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, name)
}

// fingerprints returns the distinct configurations declared for one sink, in order.
func (f *fakeSinkRuntime) fingerprints(name string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ensured[name]...)
}

// deletions returns the sinks withdrawn so far.
func (f *fakeSinkRuntime) deletions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
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

// slicesEqual compares two string slices element-wise, treating nil and empty as
// equal so an assertion can spell "no targets" as nil.
func slicesEqual(a, b []string) bool {
	return slices.Equal(a, b)
}
