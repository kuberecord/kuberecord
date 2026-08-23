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

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/goleak"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
)

// TestOperatorBootsIdleWithNoCRs is Task 1.10's central acceptance criterion:
// the assembled operator — every runnable, every reconciler, the real
// composition root — must boot against an API server that has the CRDs installed
// and *no* custom resources at all, report healthy and ready, log nothing at
// Error level, and shut down without leaking a goroutine.
//
// That state is not a corner case; it is what a fresh install runs in between
// `make deploy` and the first ClickHouseSink. Getting it wrong would mean an
// operator that crash-loops, or reports unready, until someone configures it —
// which is exactly the env-var-configured behavior Phase 1 removes. The three
// assertions map onto the three ways that could go wrong: the probes cover
// "reports healthy", the error-log count covers "does nothing noisy", and goleak
// covers "the runnables it started actually stop".
//
// It deliberately dials no ClickHouse: with no sink CR, the sink runtime holds
// no instances, so nothing in the process has a backend to reach.
func TestOperatorBootsIdleWithNoCRs(t *testing.T) {
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	if dir := firstEnvtestBinaryDir(); dir != "" {
		testEnv.BinaryAssetsDirectory = dir
	}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("start envtest (run `make setup-envtest`): %v", err)
	}
	t.Cleanup(func() {
		if stopErr := testEnv.Stop(); stopErr != nil {
			t.Errorf("stop envtest: %v", stopErr)
		}
	})

	// Snapshot the goroutines the test harness itself holds — envtest's apiserver
	// and etcd supervisors included — so what remains at the end is the operator's
	// own. The check is a defer rather than a t.Cleanup so it runs while envtest
	// is still up, i.e. against the same baseline it snapshotted.
	defer goleak.VerifyNone(t,
		goleak.IgnoreCurrent(),
		// client-go keeps idle connections (and their read/write pumps) alive past
		// the manager's shutdown; they belong to the shared transport cache, not to
		// any runnable this test started.
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreAnyFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreAnyFunction("k8s.io/klog/v2.(*loggingT).flushDaemon"),
	)

	// Every log line the operator emits is counted by severity, so "no error
	// logs" is an assertion rather than something a human has to eyeball.
	errorLog := newCountingSink()
	ctrl.SetLogger(logr.New(errorLog))

	probeAddr := freeLocalAddr(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		t.Fatalf("build the manager: %v", err)
	}

	if err := setupOperator(mgr, operatorConfig{
		clusterID:         "envtest-cluster",
		operatorNamespace: "kuberecord-system",
		pipelineWorkers:   2,
		writer: writerTuning{
			queueSize: 16, workers: 1, batchMaxRows: 8,
			batchMaxWait: time.Second, enqueueTimeout: time.Second, drainTimeout: time.Second,
		},
	}); err != nil {
		t.Fatalf("wire up the operator: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()

	select {
	case err := <-stopped:
		cancel()
		t.Fatalf("the manager returned before the test could probe it: %v", err)
	case <-mgr.Elected():
	}

	// Wait for every type the control plane watches to be cached before probing.
	// A cache-backed List blocks until its informer has synced, so this both
	// asserts that all six come up against a cluster holding none of them, and
	// closes the window in which cancelling below would interrupt a sync in
	// flight — an interrupted sync logs at Error, and the assertion at the end of
	// this test is that nothing does.
	//
	// Every watched type has to be here, sink kinds included: a type left out is
	// one whose informer may still be syncing when the cancellation lands, and the
	// error it then logs looks like a fault in whatever *other* source that
	// controller was still bringing up.
	for _, list := range []client.ObjectList{
		&corev1.SecretList{},
		&corev1.NamespaceList{},
		&v1alpha1.ClickHouseSinkList{},
		&v1alpha1.S3SinkList{},
		&v1alpha1.StreamRuleList{},
		&v1alpha1.ClusterStreamRuleList{},
	} {
		if err := mgr.GetClient().List(ctx, list); err != nil {
			cancel()
			t.Fatalf("list %T through the operator's cache: %v", list, err)
		}
	}

	for _, probe := range []string{"healthz", "readyz"} {
		assertProbeOK(t, fmt.Sprintf("http://%s/%s", probeAddr, probe))
	}

	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("the manager stopped with an error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the manager did not stop within 30s of its context being cancelled")
	}

	if lines := errorLog.errors(); len(lines) > 0 {
		t.Errorf("the operator logged %d error(s) while idle:\n\t%v", len(lines), lines)
	}
}

// assertProbeOK requires the probe endpoint to answer 200. Readiness is a plain
// ping by design (a cluster with no sinks is a valid state), so there is nothing
// to wait for — a non-200 here means the check itself is misconfigured.
func assertProbeOK(t *testing.T, url string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build the probe request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Errorf("close the probe response body: %v", closeErr)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET %s = %d, want %d", url, resp.StatusCode, http.StatusOK)
	}
}

// freeLocalAddr reserves a loopback port and hands it back as "host:port". The
// probe server is bound by address rather than by a fixed port so this test
// cannot collide with anything else on the machine.
func freeLocalAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a local port: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release the reserved port: %v", err)
	}
	return addr
}

// countingSink is a logr.LogSink that records every Error-level line the whole
// process emits.
//
// It is a sink rather than a buffer-scraping zap encoder because the assertion is
// about severity, not text: an operator that is idle should have nothing to say
// at Error level, and a substring match over a formatted stream would be one
// message-wording change away from silently passing.
//
// Derived loggers (WithName/WithValues) share one record — the count is
// process-wide — but each carries its own name, so a failure names the component
// that logged rather than whichever one happened to derive last.
type countingSink struct {
	record *errorRecord
	name   string
}

// errorRecord is the shared, concurrency-safe tail of error lines. Loggers are
// derived from many goroutines, so the storage is a pointer the derivations share
// rather than state any one of them owns.
type errorRecord struct {
	mu    sync.Mutex
	lines []string
}

func newCountingSink() *countingSink { return &countingSink{record: &errorRecord{}} }

func (s *countingSink) Init(logr.RuntimeInfo) {}

// Enabled reports true for every V-level: the operator's own Info logging is
// discarded here anyway, and enabling everything keeps the sink from hiding a
// V-level call that panics.
func (s *countingSink) Enabled(int) bool { return true }

func (s *countingSink) Info(int, string, ...any) {}

func (s *countingSink) Error(err error, msg string, kv ...any) {
	s.record.mu.Lock()
	defer s.record.mu.Unlock()
	s.record.lines = append(s.record.lines, fmt.Sprintf("[%s] %s: %v %v", s.name, msg, err, kv))
}

func (s *countingSink) WithValues(...any) logr.LogSink { return s }

func (s *countingSink) WithName(name string) logr.LogSink {
	derived := *s
	if derived.name == "" {
		derived.name = name
	} else {
		derived.name = derived.name + "." + name
	}
	return &derived
}

// errors returns a copy of the recorded error lines.
func (s *countingSink) errors() []string {
	s.record.mu.Lock()
	defer s.record.mu.Unlock()
	return append([]string(nil), s.record.lines...)
}

// firstEnvtestBinaryDir locates a downloaded envtest binary directory so this
// test also runs straight from an IDE, where KUBEBUILDER_ASSETS is not set by the
// Makefile. It mirrors the helpers in api/v1alpha1 and internal/controller; an
// empty result simply leaves envtest to its own KUBEBUILDER_ASSETS lookup.
func firstEnvtestBinaryDir() string {
	basePath := filepath.Join("..", "bin", "k8s")
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
