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

package watch

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// restCfg points at the envtest API server shared by every test in this
// package.
//
// Resolution is a question only a real API server can answer honestly — the
// whole point of the resolver is to survive the shapes discovery actually
// returns, which a hand-written mapper cannot reproduce faithfully. One
// package-wide apiserver keeps that affordable: booting envtest dominates these
// tests' runtime by orders of magnitude.
var restCfg *rest.Config

// TestMain boots one envtest apiserver for the package. No CRDs are installed
// up front: the kinds these tests resolve are either built in, or (for the
// self-healing test) installed part-way through on purpose.
func TestMain(m *testing.M) {
	os.Exit(runTestsWithEnvtest(m))
}

// runTestsWithEnvtest exists so the envtest teardown runs through a deferred
// call rather than racing os.Exit, which never runs defers.
func runTestsWithEnvtest(m *testing.M) (code int) {
	testEnv := &envtest.Environment{}
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
	restCfg = cfg

	return m.Run()
}

// firstEnvtestBinaryDir locates a downloaded envtest binary directory so these
// tests also run straight from an IDE, where KUBEBUILDER_ASSETS is not set by
// the Makefile. It mirrors the helper in the api/v1alpha1 suite; an empty result
// simply leaves envtest to its own KUBEBUILDER_ASSETS lookup.
func firstEnvtestBinaryDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}

// newRESTMapper builds a *fresh* lazy dynamic REST mapper — the exact type
// mgr.GetRESTMapper() hands the operator on the pinned controller-runtime.
//
// Freshness per test matters: the mapper accumulates discovery state, and a
// test that proves "the mapper re-discovers after a miss" is worthless if some
// earlier test already warmed the group into its cache.
func newRESTMapper(t *testing.T) meta.RESTMapper {
	t.Helper()

	httpClient, err := rest.HTTPClientFor(restCfg)
	if err != nil {
		t.Fatalf("building an HTTP client for envtest: %v", err)
	}
	mapper, err := apiutil.NewDynamicRESTMapper(restCfg, httpClient)
	if err != nil {
		t.Fatalf("building a dynamic REST mapper: %v", err)
	}
	return mapper
}

// The GVKs under test. Pod and Deployment are the two the acceptance criteria
// name; Namespace is the built-in cluster-scoped kind the scope classifier is
// checked against; Widget is backed by testdata/widget_crd.yaml and does not
// exist until the self-healing test installs it.
var (
	podGVK        = schema.GroupVersionKind{Version: "v1", Kind: "Pod"}
	deploymentGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	namespaceGVK  = schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}
	widgetGVK     = schema.GroupVersionKind{Group: "test.kuberecord.io", Version: "v1", Kind: "Widget"}

	podGVR        = schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	deploymentGVR = deploymentGVK.GroupVersion().WithResource("deployments")
	namespaceGVR  = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	widgetGVR     = widgetGVK.GroupVersion().WithResource("widgets")
)

// fakeMapper is a REST mapper whose single answer the test dictates.
//
// meta.RESTMapper is embedded rather than implemented: the resolver only ever
// calls RESTMapping, and leaving the other six methods to panic on a nil
// interface is a louder failure than a silent zero value if that ever stops
// being true.
type fakeMapper struct {
	meta.RESTMapper

	mu    sync.Mutex
	calls int
	fn    func() (*meta.RESTMapping, error)
}

func (f *fakeMapper) RESTMapping(_ schema.GroupKind, _ ...string) (*meta.RESTMapping, error) {
	f.mu.Lock()
	f.calls++
	fn := f.fn
	f.mu.Unlock()
	return fn()
}

// callCount reports how many times the resolver actually reached the mapper —
// the observable that proves the retry gate is doing anything at all.
func (f *fakeMapper) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeMapper) setFn(fn func() (*meta.RESTMapping, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fn = fn
}

// fakeClock drives the retry gate so its schedule can be asserted to the
// nanosecond instead of being slept through.
type fakeClock struct {
	mu      sync.Mutex
	instant time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.instant
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.instant = c.instant.Add(d)
}

// noKindMatch is the error a REST mapper returns for a kind it has never heard
// of within a group it does serve.
func noKindMatch(gvk schema.GroupVersionKind) error {
	return &meta.NoKindMatchError{GroupKind: gvk.GroupKind(), SearchedVersions: []string{gvk.Version}}
}

// isKindNotFound reports whether err is the resolver's typed "kind is not
// installed" verdict, exercising the errors.As-ability the acceptance criteria
// require.
func isKindNotFound(err error) bool {
	var notFound *ErrKindNotFound
	return errors.As(err, &notFound)
}

// TestCheckScope covers the scope classifier in isolation: which rule scope may
// watch which resource scope.
func TestCheckScope(t *testing.T) {
	cases := []struct {
		name        string
		gvk         schema.GroupVersionKind
		gvr         schema.GroupVersionResource
		namespaced  bool
		rule        RuleScope
		wantRefused bool
	}{
		{
			name: "namespaced rule may watch a namespaced kind",
			gvk:  podGVK, gvr: podGVR, namespaced: true, rule: NamespacedRule,
		},
		{
			name: "namespaced rule may not watch a cluster-scoped kind",
			gvk:  namespaceGVK, gvr: namespaceGVR, namespaced: false, rule: NamespacedRule,
			wantRefused: true,
		},
		{
			name: "cluster rule may watch a namespaced kind",
			gvk:  podGVK, gvr: podGVR, namespaced: true, rule: ClusterRule,
		},
		{
			name: "cluster rule may watch a cluster-scoped kind",
			gvk:  namespaceGVK, gvr: namespaceGVR, namespaced: false, rule: ClusterRule,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkScope(tc.gvk, tc.gvr, tc.namespaced, tc.rule)

			if !tc.wantRefused {
				if err != nil {
					t.Fatalf("checkScope() = %v, want nil", err)
				}
				return
			}

			var scopeErr *ErrClusterScopedKind
			if !errors.As(err, &scopeErr) {
				t.Fatalf("checkScope() = %v, want an *ErrClusterScopedKind", err)
			}
			if scopeErr.GVK != tc.gvk {
				t.Errorf("error carries GVK %s, want %s", scopeErr.GVK, tc.gvk)
			}
			if scopeErr.Resource != tc.gvr {
				t.Errorf("error carries resource %s, want %s", scopeErr.Resource, tc.gvr)
			}
			// The message has to tell the rule's author what to do instead;
			// this condition is surfaced verbatim on the CR (Task 1.7).
			if !strings.Contains(scopeErr.Error(), "ClusterStreamRule") {
				t.Errorf("error message %q does not point at ClusterStreamRule", scopeErr.Error())
			}
		})
	}
}

// TestResolveClassifiesMapperErrors pins down which mapper failures mean "the
// kind is not installed" (self-healing, rule-level) and which mean "discovery is
// broken" (operational, must not be blamed on the rule).
func TestResolveClassifiesMapperErrors(t *testing.T) {
	// The shape the lazy mapper returns for an entirely unknown API group: a
	// multi-error whose Unwrap() []error yields a NoResourceMatchError.
	discoveryFailed := apiutil.ErrResourceDiscoveryFailed{
		widgetGVK.GroupVersion(): apierrors.NewNotFound(
			schema.GroupResource{Group: widgetGVK.Group, Resource: "widgets"}, "",
		),
	}
	outage := errors.New("Get \"https://10.0.0.1:6443/api\": dial tcp: connection refused")

	cases := []struct {
		name         string
		mapperErr    error
		wantNotFound bool
	}{
		{
			name:         "unknown kind inside a served group",
			mapperErr:    noKindMatch(widgetGVK),
			wantNotFound: true,
		},
		{
			name:         "unknown resource",
			mapperErr:    &meta.NoResourceMatchError{PartialResource: widgetGVR},
			wantNotFound: true,
		},
		{
			name:         "unknown group, reported as a discovery multi-error",
			mapperErr:    &discoveryFailed,
			wantNotFound: true,
		},
		{
			name:      "api server unreachable",
			mapperErr: outage,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := NewResolver(&fakeMapper{fn: func() (*meta.RESTMapping, error) {
				return nil, tc.mapperErr
			}})

			gvr, namespaced, err := resolver.Resolve(widgetGVK)
			if err == nil {
				t.Fatalf("Resolve() unexpectedly succeeded with %s (namespaced=%t)", gvr, namespaced)
			}
			if !errors.Is(err, tc.mapperErr) {
				t.Errorf("Resolve() = %v, which does not unwrap to the mapper's error %v", err, tc.mapperErr)
			}

			var notFound *ErrKindNotFound
			if got := errors.As(err, &notFound); got != tc.wantNotFound {
				t.Fatalf("errors.As(%v, *ErrKindNotFound) = %t, want %t", err, got, tc.wantNotFound)
			}
			if !tc.wantNotFound {
				// A transient failure must still name the kind it was asked
				// about, or the operator cannot tell which rule stalled.
				if !strings.Contains(err.Error(), widgetGVK.Kind) {
					t.Errorf("passthrough error %q does not name kind %q", err, widgetGVK.Kind)
				}
				return
			}
			if notFound.GVK != widgetGVK {
				t.Errorf("error carries GVK %s, want %s", notFound.GVK, widgetGVK)
			}
		})
	}
}

// TestResolveBackoffGate proves the re-resolution policy: a parked kind is
// retried on a widening interval capped at maxRetryDelay, attempts arriving
// early never reach discovery, and a success clears the penalty.
func TestResolveBackoffGate(t *testing.T) {
	clock := &fakeClock{instant: time.Unix(0, 0)}
	mapper := &fakeMapper{fn: func() (*meta.RESTMapping, error) { return nil, noKindMatch(widgetGVK) }}
	resolver := NewResolver(mapper)
	resolver.now = clock.now

	// One entry per attempt that reaches the mapper, giving the interval that
	// attempt's failure installs. Doubling from baseRetryDelay, clamped at
	// maxRetryDelay and then held there.
	wantDelays := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}

	for attempt, want := range wantDelays {
		if _, _, err := resolver.Resolve(widgetGVK); !isKindNotFound(err) {
			t.Fatalf("attempt %d: Resolve() = %v, want ErrKindNotFound", attempt, err)
		}
		if got := mapper.callCount(); got != attempt+1 {
			t.Fatalf("attempt %d: mapper consulted %d times, want %d", attempt, got, attempt+1)
		}

		// Everything strictly before retryAt is answered from the gate: the
		// verdict is replayed and discovery is left alone.
		clock.advance(want - time.Nanosecond)
		if _, _, err := resolver.Resolve(widgetGVK); !isKindNotFound(err) {
			t.Fatalf("attempt %d: gated Resolve() = %v, want the parked ErrKindNotFound", attempt, err)
		}
		if got := mapper.callCount(); got != attempt+1 {
			t.Fatalf("attempt %d: gate let %d calls through after %s, want %d — backoff is not %s",
				attempt, got, want-time.Nanosecond, attempt+1, want)
		}

		// ...and at retryAt itself the gate opens again.
		clock.advance(time.Nanosecond)
	}

	// A success unparks the kind entirely.
	mapper.setFn(func() (*meta.RESTMapping, error) {
		return &meta.RESTMapping{Resource: widgetGVR, Scope: meta.RESTScopeNamespace}, nil
	})
	gvr, namespaced, err := resolver.Resolve(widgetGVK)
	if err != nil {
		t.Fatalf("Resolve() after the kind appeared = %v, want success", err)
	}
	if gvr != widgetGVR || !namespaced {
		t.Fatalf("Resolve() = (%s, namespaced=%t), want (%s, namespaced=true)", gvr, namespaced, widgetGVR)
	}
	if len(resolver.parked) != 0 {
		t.Fatalf("a successful resolution left %d parked entries, want none", len(resolver.parked))
	}

	// So the next failure starts over at baseRetryDelay rather than inheriting
	// the 30-second penalty the kind had accumulated.
	mapper.setFn(func() (*meta.RESTMapping, error) { return nil, noKindMatch(widgetGVK) })
	before := mapper.callCount()
	if _, _, err := resolver.Resolve(widgetGVK); !isKindNotFound(err) {
		t.Fatalf("Resolve() after the kind disappeared again = %v, want ErrKindNotFound", err)
	}
	if got := mapper.callCount(); got != before+1 {
		t.Fatalf("an unparked kind was gated: mapper consulted %d times, want %d", got, before+1)
	}
	clock.advance(baseRetryDelay)
	if _, _, err := resolver.Resolve(widgetGVK); !isKindNotFound(err) {
		t.Fatalf("Resolve() = %v, want ErrKindNotFound", err)
	}
	if got := mapper.callCount(); got != before+2 {
		t.Fatalf("gate did not reopen after %s: mapper consulted %d times, want %d", baseRetryDelay, got, before+2)
	}
}

// TestResolveForScopeRefusesClusterScopedKind checks the classifier is actually
// wired into the resolver's reconciler-facing entry point, and that a scope
// violation is never parked — it cannot heal on its own, so gating it would only
// delay the next honest answer.
func TestResolveForScopeRefusesClusterScopedKind(t *testing.T) {
	mapper := &fakeMapper{fn: func() (*meta.RESTMapping, error) {
		return &meta.RESTMapping{Resource: namespaceGVR, Scope: meta.RESTScopeRoot}, nil
	}}
	resolver := NewResolver(mapper)

	if _, _, err := resolver.ResolveForScope(namespaceGVK, NamespacedRule); !errors.As(err, new(*ErrClusterScopedKind)) {
		t.Fatalf("ResolveForScope(namespaced rule, cluster-scoped kind) = %v, want *ErrClusterScopedKind", err)
	}
	if len(resolver.parked) != 0 {
		t.Errorf("a scope violation parked %d kinds, want none", len(resolver.parked))
	}

	gvr, namespaced, err := resolver.ResolveForScope(namespaceGVK, ClusterRule)
	if err != nil {
		t.Fatalf("ResolveForScope(cluster rule, cluster-scoped kind) = %v, want success", err)
	}
	if gvr != namespaceGVR || namespaced {
		t.Fatalf("ResolveForScope() = (%s, namespaced=%t), want (%s, namespaced=false)", gvr, namespaced, namespaceGVR)
	}
}

// TestResolveConcurrent hammers one resolver from many goroutines over a mapper
// that alternates between success and failure, so park and unpark race on the
// same keys. It is the -race guard on the gate's bookkeeping.
func TestResolveConcurrent(t *testing.T) {
	var answers atomic.Int64
	mapper := &fakeMapper{fn: func() (*meta.RESTMapping, error) {
		if answers.Add(1)%2 == 0 {
			return nil, noKindMatch(widgetGVK)
		}
		return &meta.RESTMapping{Resource: widgetGVR, Scope: meta.RESTScopeNamespace}, nil
	}}
	resolver := NewResolver(mapper)
	// Collapse the gate so parking and unparking both stay on the hot path
	// instead of the first failure silencing every later attempt.
	resolver.baseDelay = 0
	resolver.maxDelay = 0

	gvks := []schema.GroupVersionKind{podGVK, deploymentGVK, namespaceGVK, widgetGVK}

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			gvk := gvks[i%len(gvks)]
			for range 20 {
				if _, _, err := resolver.Resolve(gvk); err != nil && !isKindNotFound(err) {
					t.Errorf("Resolve(%s) = %v, want success or ErrKindNotFound", gvk, err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestResolveAgainstAPIServer is the envtest half of the acceptance criteria:
// real discovery data, real mapper, real error shapes.
func TestResolveAgainstAPIServer(t *testing.T) {
	mapper := newRESTMapper(t)

	cases := []struct {
		name           string
		gvk            schema.GroupVersionKind
		wantGVR        schema.GroupVersionResource
		wantNamespaced bool
		wantNotFound   bool
	}{
		{
			name: "core v1 Pod", gvk: podGVK,
			wantGVR: podGVR, wantNamespaced: true,
		},
		{
			name: "apps v1 Deployment", gvk: deploymentGVK,
			wantGVR: deploymentGVR, wantNamespaced: true,
		},
		{
			name: "cluster-scoped core v1 Namespace", gvk: namespaceGVK,
			wantGVR: namespaceGVR,
		},
		{
			name:         "unknown kind inside a served group",
			gvk:          schema.GroupVersionKind{Version: "v1", Kind: "Nonexistent"},
			wantNotFound: true,
		},
		{
			name:         "unknown group",
			gvk:          schema.GroupVersionKind{Group: "nosuch.example.com", Version: "v1", Kind: "Widget"},
			wantNotFound: true,
		},
		{
			// Listed last: on a miss the mapper drops its cached entry for the
			// group, so this case makes later lookups of apps/v1 pay for a
			// rediscovery they should not have to.
			name:         "unserved version of a served group",
			gvk:          schema.GroupVersionKind{Group: deploymentGVK.Group, Version: "v99", Kind: deploymentGVK.Kind},
			wantNotFound: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh resolver per case: the retry gate is per-resolver state and
			// must not let one case's parked verdict answer another's question.
			gvr, namespaced, err := NewResolver(mapper).Resolve(tc.gvk)

			if tc.wantNotFound {
				if !isKindNotFound(err) {
					t.Fatalf("Resolve(%s) = (%s, %t, %v), want ErrKindNotFound", tc.gvk, gvr, namespaced, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve(%s) = %v, want success", tc.gvk, err)
			}
			if gvr != tc.wantGVR {
				t.Errorf("Resolve(%s) resolved to %s, want %s", tc.gvk, gvr, tc.wantGVR)
			}
			if namespaced != tc.wantNamespaced {
				t.Errorf("Resolve(%s) reported namespaced=%t, want %t", tc.gvk, namespaced, tc.wantNamespaced)
			}
		})
	}
}

// TestResolveForScopeAgainstAPIServer repeats the scope rules against real
// discovery data, so a change in how the mapper reports scope cannot pass the
// hand-written classifier test while breaking production.
func TestResolveForScopeAgainstAPIServer(t *testing.T) {
	mapper := newRESTMapper(t)

	cases := []struct {
		name        string
		gvk         schema.GroupVersionKind
		rule        RuleScope
		wantRefused bool
	}{
		{name: "StreamRule watching Pods", gvk: podGVK, rule: NamespacedRule},
		{name: "StreamRule watching Namespaces", gvk: namespaceGVK, rule: NamespacedRule, wantRefused: true},
		{name: "ClusterStreamRule watching Pods", gvk: podGVK, rule: ClusterRule},
		{name: "ClusterStreamRule watching Namespaces", gvk: namespaceGVK, rule: ClusterRule},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := NewResolver(mapper).ResolveForScope(tc.gvk, tc.rule)

			if !tc.wantRefused {
				if err != nil {
					t.Fatalf("ResolveForScope(%s) = %v, want success", tc.gvk, err)
				}
				return
			}
			if !errors.As(err, new(*ErrClusterScopedKind)) {
				t.Fatalf("ResolveForScope(%s) = %v, want *ErrClusterScopedKind", tc.gvk, err)
			}
		})
	}
}

// TestResolveSelfHealsWhenCRDIsInstalled is the lifecycle acceptance criterion:
// a rule may legitimately precede its CRD, so a kind that is missing at first
// ask must resolve once the CRD lands — same process, same mapper, no restart.
//
// It runs on the production backoff schedule (real clock, real constants) so
// what it proves is the behaviour an operator actually gets.
func TestResolveSelfHealsWhenCRDIsInstalled(t *testing.T) {
	// A dedicated mapper: this test's whole claim is that the *same* mapper
	// instance that cached "no such group" refreshes itself afterwards.
	resolver := NewResolver(newRESTMapper(t))

	if _, _, err := resolver.Resolve(widgetGVK); !isKindNotFound(err) {
		t.Fatalf("before the CRD exists, Resolve(%s) = %v, want ErrKindNotFound", widgetGVK, err)
	}

	crds, err := envtest.InstallCRDs(restCfg, envtest.CRDInstallOptions{
		Paths:              []string{filepath.Join("testdata", "widget_crd.yaml")},
		ErrorIfPathMissing: true,
	})
	if err != nil {
		t.Fatalf("installing the Widget CRD: %v", err)
	}
	t.Cleanup(func() {
		if err := envtest.UninstallCRDs(restCfg, envtest.CRDInstallOptions{CRDs: crds}); err != nil {
			t.Errorf("cleanup: uninstalling the Widget CRD failed: %v", err)
		}
	})

	// Poll for two full backoff ceilings. In practice the first attempt past the
	// initial one-second park succeeds; the generous deadline only keeps a slow
	// CI machine from turning a pass into a flake.
	deadline := time.Now().Add(2 * maxRetryDelay)
	for {
		gvr, namespaced, err := resolver.Resolve(widgetGVK)
		if err == nil {
			if gvr != widgetGVR {
				t.Fatalf("Resolve(%s) resolved to %s, want %s", widgetGVK, gvr, widgetGVR)
			}
			if !namespaced {
				t.Fatalf("Resolve(%s) reported namespaced=false, want true", widgetGVK)
			}
			return
		}
		if !isKindNotFound(err) {
			t.Fatalf("Resolve(%s) = %v, want ErrKindNotFound while parked", widgetGVK, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Resolve(%s) never self-healed within %s of the CRD being installed", widgetGVK, 2*maxRetryDelay)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
