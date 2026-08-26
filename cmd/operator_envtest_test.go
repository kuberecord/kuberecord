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
	"strings"
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

	"github.com/kuberecord/kuberecord/api/v1alpha1"
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
// The error-log count is taken while the operator is *idle*, before shutdown is
// asked for, because that is the claim; the lines controller-runtime logs about
// its own cancellation are held to a separate, narrower assertion — see
// shutdownRaceLog.
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

	// The namespace the operator believes it runs in. It is one value rather than
	// two literals because the manager's cache and the operator's own configuration
	// have to agree about it: the cache confines the Secret informer to this
	// namespace, and the reconcilers resolve credentialsSecretRef against it.
	const operatorNamespace = "kuberecord-system"

	probeAddr := freeLocalAddr(t)
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		// The cache main() builds, not the default one. It matters here more than
		// anywhere: managerCacheOptions confines the Secret informer to the
		// operator's own namespace because the operator's Secret grant is a
		// namespaced Role (Task 1.9, D7), and its doc comment notes that getting
		// this wrong is invisible to envtest — whose client is an administrator. A
		// test that claims to boot the real composition root should at least boot it
		// with the real manager options.
		Cache:                  managerCacheOptions(operatorNamespace),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		t.Fatalf("build the manager: %v", err)
	}

	if err := setupOperator(mgr, operatorConfig{
		clusterID:         "envtest-cluster",
		operatorNamespace: operatorNamespace,
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

	// The central assertion, and it is made *here* — before shutdown is asked for —
	// because "while idle" is what it is about. An idle operator logging at Error is
	// a fault; a shutting-down one is judged by the narrower rule below.
	idle := errorLog.errors()
	if len(idle) > 0 {
		t.Errorf("the operator logged %d error(s) while idle:\n\t%v", len(idle), idle)
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

	// Shutdown is still held to an assertion, just a different one: everything
	// logged from here on must be controller-runtime reporting its own cancellation,
	// and nothing else. A real error on the way down — a sink client that could not
	// be closed, a drain that failed, a runnable returning a fault — still fails.
	for _, line := range errorLog.errors()[len(idle):] {
		if shutdownRaceLog(line) {
			continue
		}
		t.Errorf("the operator logged an error while shutting down:\n\t%s", line)
	}
}

// shutdownRaceLog reports whether an error line is controller-runtime saying it was
// cancelled in the middle of starting a watch, rather than anything the operator
// did.
//
// All three are the framework's own, logged on the path between "a controller
// starts its sources" and "those sources have synced":
//
//   - "failed to get informer from cache" (pkg/internal/source/kind.go): the
//     source's start goroutine polls Cache.GetInformer, and a cancelled context
//     turns its pending sync into a timeout error, which it logs before noticing
//     that it is itself being shut down.
//   - "Could not wait for Cache to sync" (pkg/internal/controller/controller.go):
//     the same cancellation, seen by the controller that was waiting on the source.
//   - "error received after stop sequence was engaged" (pkg/manager/internal.go):
//     the manager's report of an error arriving from a runnable after shutdown had
//     begun, which is how the first two reach it.
//
// They are classified rather than eliminated because there is nothing here to fix.
// The operator watches Secrets and Namespaces, whose informers are created lazily
// by their controllers rather than eagerly by a field index, so a process cancelled
// milliseconds after electing itself leader can always land inside that window —
// measured at roughly one run in eight, and present in this test since well before
// the S3Sink reconciler added a second Secret watch. controller-runtime v0.23
// exposes no "this controller has started" signal to wait on, so the only
// alternative is a sleep: that would make the window narrower, not closed, and
// would still be asserting on timing.
//
// The important half of the rule is what it does *not* forgive: an unrecognised
// error during shutdown fails the test, which is what keeps this a classification
// rather than a blanket exemption for the noisiest phase of the process's life.
func shutdownRaceLog(line string) bool {
	for _, framework := range []string{
		"failed to get informer from cache",
		"Could not wait for Cache to sync",
		"error received after stop sequence was engaged",
	} {
		if strings.Contains(line, framework) {
			return true
		}
	}
	return false
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

// TestShutdownRaceLogClassification keeps shutdownRaceLog from quietly becoming a
// blanket exemption for the noisiest phase of the process's life.
//
// It is the risk that rule carries: a substring match, applied to the one window
// where the operator does the most work, in a test whose whole subject is "nothing
// is logged at Error". If the list ever grew a phrase broad enough to swallow a
// real fault — or if the framework's wording drifted and the list stopped matching
// anything at all — nothing would say so. So both directions are asserted, and
// they are asserted here rather than inside the boot test because they are true
// without a cluster.
func TestShutdownRaceLogClassification(t *testing.T) {
	// The lines controller-runtime really does emit when it is cancelled mid-start,
	// copied verbatim from observed runs of the boot test.
	forgiven := []string{
		"[controller-runtime.source.Kind] failed to get informer from cache: " +
			"Timeout: failed waiting for *v1.Secret Informer to sync []",
		"[controller-runtime.source.Kind] failed to get informer from cache: " +
			"Timeout: failed waiting for *v1.Namespace Informer to sync []",
		"[] Could not wait for Cache to sync: failed to wait for clusterstreamrule caches to sync " +
			"kind source: *v1.Namespace: cache did not sync []",
		"[] error received after stop sequence was engaged: failed to wait for clusterstreamrule " +
			"caches to sync kind source: *v1.Namespace: cache did not sync []",
	}
	for _, line := range forgiven {
		if !shutdownRaceLog(line) {
			t.Errorf("a known controller-runtime shutdown line is no longer recognised, so the boot "+
				"test is flaky again:\n\t%s", line)
		}
	}

	// The faults this test exists to keep catching: everything kuberecord itself
	// would log on the way down. Each one is a real message from the shutdown paths
	// of the sink writers and the pipeline.
	fatal := []string{
		"[s3writer] s3writer: failed to close the object store client: connection reset",
		"[s3writer] s3writer: giving up on an object after retries",
		"[chwriter] failed to close the ClickHouse connection",
		"[pipeline] Failed to drain the workqueue before shutdown",
		"[sinks] Sink instance did not stop within the drain timeout",
	}
	for _, line := range fatal {
		if shutdownRaceLog(line) {
			t.Errorf("shutdownRaceLog forgives an operator error, so a real shutdown fault would pass "+
				"unnoticed:\n\t%s", line)
		}
	}
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
