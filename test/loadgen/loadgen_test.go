//go:build integration

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

// Package loadgen is kuberecord's synthetic-churn load harness (Task 0.8,
// extended into named scale profiles by Task 2.3). It drives realistic object
// churn — create N objects across one or more kinds, then sustain M mutations/sec
// with a configurable payload size and delete ratio — through the *real*
// pipeline: envtest apiserver -> informers (with production's cache transform) ->
// internal/pipeline workqueue -> CHWriter -> a dockerized ClickHouse.
//
// What it reports is the published performance envelope (docs/PERFORMANCE.md):
// sustained records/sec, p50/p99 enqueue-block, peak hand-off queue depth, peak
// pipeline queue depth, process CPU over the churn window, peak RSS and Go heap
// footprint. A profile can also declare pass criteria, in which case the run
// fails itself rather than leaving a human to compare stdout against a table.
//
// The harness supplies its own minimal ListerRegistry and SinkRouter (see
// harnessLister / harnessRouter) rather than the production WatchManager and
// SinkManager: the figures it reports are about the *write* path, so one informer
// per kind with everything in scope is the honest, minimal rig. It does install
// production's informer transform (watch.TransformObject), because the size of a
// cached object is exactly what the memory half of these numbers describes.
//
// It is guarded by the `integration` build tag and run via `make bench-load`,
// which stands up the ClickHouse container and points CH_TEST_ADDR at it,
// exactly like `make test-integration`:
//
//	make bench-load PROFILE=massive PPROF_DIR=docs/perf/after
package loadgen

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/sink/clickhouse"
	"github.com/yelzhy/kuberecord/internal/watch"
)

// loadgenSink is the single sink name every work item routes to.
const loadgenSink = "loadgen"

// churnNamespace is where every churned object lives. One namespace keeps the
// scope count equal to the kind count, so a profile's "mixed GVKs" dimension is
// not entangled with a namespace-fan-out dimension the operator handles the same
// way anyway (one informer per (GVR, namespace) target either way).
const churnNamespace = "default"

// Harness parameters. --profile selects one of the shipped scale profiles
// (test/loadgen/profiles/); every other flag is a single-knob override on top of
// it, with a negative/zero sentinel meaning "not overridden" so the profile's own
// value is what runs. LOADGEN_* environment twins sit between the two (profile <
// env < flag) so the Makefile can forward an override without positional args.
var (
	flagProfile    = flag.String("profile", ProfileSmall, "named scale profile to run (see test/loadgen/profiles/)")
	flagProfileDir = flag.String("profile-dir", ProfilesDir, "directory holding the profile JSON files")
	flagPprofDir   = flag.String("pprof-dir", "",
		"if set, write heap/alloc pprof profiles and a run summary into this directory")

	flagObjects     = flag.Int("objects", -1, "override the profile's object count")
	flagRate        = flag.Int("rate", -1, "override the profile's sustained mutations per second")
	flagPayload     = flag.Int("payload-bytes", -1, "override the profile's per-object payload size")
	flagDuration    = flag.Duration("duration", 0, "override the profile's churn duration")
	flagDeleteRatio = flag.Float64("delete-ratio", -1, "override the profile's delete-and-recreate ratio")
	flagConcurrency = flag.Int("concurrency", -1, "override the profile's churn worker count")
	flagKinds       = flag.String("kinds", "", "override the profile's kinds (comma-separated Kind names)")
)

// resolveProfile loads the selected profile and applies the environment and flag
// overrides, in that order, so a flag always wins over an env twin and both win
// over the file.
func resolveProfile() (Profile, error) {
	p, err := LoadProfile(*flagProfileDir, *flagProfile)
	if err != nil {
		return Profile{}, err
	}

	envOverrides, err := OverridesFromEnv(os.Getenv)
	if err != nil {
		return Profile{}, err
	}
	if p, err = envOverrides.Apply(p); err != nil {
		return Profile{}, err
	}

	flagged := NoOverrides()
	flagged.Objects = *flagObjects
	flagged.Rate = *flagRate
	flagged.PayloadBytes = *flagPayload
	flagged.Duration = *flagDuration
	flagged.DeleteRatio = *flagDeleteRatio
	flagged.Concurrency = *flagConcurrency
	if *flagKinds != "" {
		for part := range strings.SplitSeq(*flagKinds, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				flagged.Kinds = append(flagged.Kinds, trimmed)
			}
		}
	}
	return flagged.Apply(p)
}

// harnessLister is a minimal pipeline.ListerRegistry over the manager's cache.
// Every scope is active for the whole run (the harness never stops watching), so
// the interesting half of the interface is the object lookup, which reads from
// the informer cache exactly as the production WatchManager does — no apiserver
// round-trip on the hot path.
type harnessLister struct {
	reader client.Reader
	// gvks maps a work key's Kind to the GVK to read it back as. The pipeline's
	// keys are version-agnostic (Invariant 7), so this is the harness's tiny
	// stand-in for the interest table's identity index.
	gvks map[string]schema.GroupVersionKind
}

func (l harnessLister) Get(ref pipeline.Key) (*unstructured.Unstructured, bool, bool, error) {
	gvk, known := l.gvks[ref.Kind]
	if !known {
		// No informer for this kind: the honest answer is "not in scope", which
		// makes the pipeline drop the item rather than record a phantom delete.
		return nil, false, false, nil
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	err := l.reader.Get(context.Background(), client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, obj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, true, nil
		}
		return nil, false, true, err
	}
	return obj, true, true, nil
}

// harnessRouter is a pipeline.SinkRouter over the one CHWriter this harness
// opens.
type harnessRouter struct {
	writer sink.Writer
}

func (r harnessRouter) WriterFor(id sink.ID) (sink.Writer, bool) {
	// The pipeline routes on sink.ID (Task 4.1); Key.Sink is still the bare name
	// the harness authors its keys with, lifted onto the ClickHouseSink kind.
	if id != (sink.ID{Kind: sink.DefaultSinkKind, Name: loadgenSink}) {
		return nil, false
	}
	return r.writer, true
}

// harnessMetrics implements clickhouse.Metrics. It captures the exact figures
// the report needs — every enqueue-block sample (for true percentiles), the
// peak queue depth, and the settled-write count — directly at the source,
// rather than scraping and interpolating a Prometheus histogram. All methods
// are called concurrently by the CHWriter workers and the pipeline hot path,
// so every field is mutex-guarded.
type harnessMetrics struct {
	mu             sync.Mutex
	enqueueBlocks  []float64
	peakQueueDepth float64
	settledOK      int64
	settledFailed  int64
}

func (m *harnessMetrics) ObserveEnqueueBlock(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.enqueueBlocks = append(m.enqueueBlocks, seconds)
}

func (m *harnessMetrics) SetWriteQueueDepth(n float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n > m.peakQueueDepth {
		m.peakQueueDepth = n
	}
}

func (m *harnessMetrics) IncWrite(outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if outcome == "success" {
		m.settledOK++
	} else {
		m.settledFailed++
	}
}

// The remaining clickhouse.Metrics methods are not part of the report; they are
// satisfied as no-ops so the harness need not depend on a Prometheus registry.
func (m *harnessMetrics) SetWriteQueueCapacity(float64) {}
func (m *harnessMetrics) IncEnqueueTimeout()            {}
func (m *harnessMetrics) ObserveWriteLatency(float64)   {}
func (m *harnessMetrics) IncWriteRetryAttempt()         {}
func (m *harnessMetrics) ObserveWriteBatchRows(float64) {}

// settled returns the current success and failed write counts.
func (m *harnessMetrics) settled() (ok, failed int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settledOK, m.settledFailed
}

// percentiles returns the p50 and p99 of the enqueue-block samples collected
// since the given sample index, so the churn window's latency is not diluted by
// the create phase's.
func (m *harnessMetrics) percentiles(since int) (p50, p99 float64) {
	m.mu.Lock()
	var samples []float64
	if since < len(m.enqueueBlocks) {
		samples = append(samples, m.enqueueBlocks[since:]...)
	}
	m.mu.Unlock()
	if len(samples) == 0 {
		return 0, 0
	}
	sort.Float64s(samples)
	return quantile(samples, 0.50), quantile(samples, 0.99)
}

// sampleCount is how many enqueue-block samples have been collected so far, used
// to mark the boundary between the create and churn phases.
func (m *harnessMetrics) sampleCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.enqueueBlocks)
}

func (m *harnessMetrics) peakDepth() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peakQueueDepth
}

// quantile returns the q-quantile of an already-sorted slice using the
// nearest-rank method — exact for the sample counts this harness collects, with
// no interpolation to reason about.
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// TestLoadGenChurn is the harness entry point. It is a test (not a standalone
// main) so it can bootstrap envtest exactly as the controller suite does and be
// invoked through the same `go test -tags=integration` path the Makefile
// already uses for integration coverage.
func TestLoadGenChurn(t *testing.T) {
	profile, err := resolveProfile()
	if err != nil {
		t.Fatalf("resolve load profile: %v", err)
	}

	logf.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.UseDevMode(true)))

	// Go samples one allocation per 512 KiB by default, which is far too coarse
	// here: a 60-second churn window at a few hundred records/sec leaves single
	// digits of samples on any individual call site, so a real 25% improvement in
	// the hot path is indistinguishable from sampling noise. Profiling runs
	// therefore sample 16× finer. It is set only when profiles are being written,
	// because the accounting is not free and the published envelope should describe
	// an unprofiled operator.
	if *flagPprofDir != "" {
		runtime.MemProfileRate = 32 * 1024
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kinds, err := resolveKinds(profile.Kinds)
	if err != nil {
		t.Fatalf("resolve profile kinds: %v", err)
	}

	// --- envtest apiserver ---
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("start envtest (is KUBEBUILDER_ASSETS set? run via `make bench-load`): %v", err)
	}
	defer func() {
		if stopErr := testEnv.Stop(); stopErr != nil {
			t.Logf("envtest stop: %v", stopErr)
		}
	}()

	// --- dockerized ClickHouse (auto-create the schema-v1 DDL on connect) ---
	chCfg := clickhouse.Config{
		Addr:             envOr("CH_TEST_ADDR", "127.0.0.1:9000"),
		Database:         envOr("CH_TEST_DB", "default"),
		Username:         envOr("CH_TEST_USER", "default"),
		Password:         os.Getenv("CH_TEST_PASSWORD"),
		DialTimeout:      5 * time.Second,
		ReadTimeout:      10 * time.Second,
		AutoCreateSchema: true,
	}
	metrics := &harnessMetrics{}
	chWriter, err := clickhouse.Open(chCfg, metrics)
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	// Clean the throwaway tables on the way out so a re-targeted persistent
	// ClickHouse starts fresh; the dockerized default target is discarded anyway.
	defer cleanupClickHouse(chCfg)

	// --- manager + real pipeline over the profile's kinds ---
	// DefaultTransform is production's informer transform: managedFields is
	// harvested into the actors annotation and then dropped before an object is
	// ever cached. It is installed here because the RSS figure this harness
	// publishes is largely the size of the informer caches, and measuring
	// untransformed objects would overstate the operator's real footprint (and
	// understate normalizeObject's, which would then have to strip the field
	// itself on every event).
	//
	// DefaultUnsafeDisableDeepCopy mirrors the other half of production's read
	// path: WatchManager.Get hands the pipeline the informer's *own* cached
	// instance and documents that callers must not mutate it (the pipeline never
	// does). A harness whose lister deep-copied every object on every read would
	// both overstate the operator's allocation rate and mask the copy the hot path
	// itself does — which is precisely what Task 2.3's profiles are looking at.
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme.Scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache: cache.Options{
			DefaultTransform:             watch.TransformObject,
			DefaultUnsafeDisableDeepCopy: ptr.To(true),
		},
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	// The writer is a plain manager.Runnable: its Start applies the shipped DDL
	// (AutoCreateSchema above) before draining, exactly as a sink instance built
	// by the SinkManager does in production.
	if err := mgr.Add(chWriter); err != nil {
		t.Fatalf("add the ClickHouse writer to the manager: %v", err)
	}

	// A dedicated registry (rather than the process-wide instance) so the run's
	// own pipeline counters — hashCache size, dedup skips, dropped items — can be
	// gathered into the report: they are what makes the RSS figure attributable
	// and what proves the run did the work it claims.
	registry := prometheus.NewRegistry()
	pipe, err := pipeline.New(pipeline.Options{
		ClusterID: loadgenSink,
		Workers:   pipeline.DefaultWorkers,
		Lister:    harnessLister{reader: mgr.GetCache(), gvks: gvksByKind(kinds)},
		Router:    harnessRouter{writer: chWriter},
		Metrics:   pipeline.NewPipelineMetrics(registry),
	})
	if err != nil {
		t.Fatalf("build pipeline: %v", err)
	}
	// Every scope is warm from the start: this harness measures throughput, and
	// leaving them cold would tag every create as Snapshot instead of Added. That
	// is a labelling difference only — the write path does identical work — but
	// the reported figures should describe the steady state, not a boot state.
	for _, kind := range kinds {
		pipe.MarkScopeWarm(loadgenSink, pipeline.ScopeKey{
			Group: kind.GVK.Group, Kind: kind.GVK.Kind, Namespace: churnNamespace,
		})
	}
	if err := mgr.Add(pipe); err != nil {
		t.Fatalf("add pipeline to manager: %v", err)
	}

	// Feed the pipeline from one raw informer per kind on the manager's cache.
	// This is the harness's stand-in for the WatchManager's handlers (Task 1.4):
	// same event-to-key translation, without the dynamic lifecycle machinery.
	for _, kind := range kinds {
		if err := watchKind(ctx, mgr, pipe, kind, t); err != nil {
			t.Fatalf("watch %s: %v", kind.GVK.Kind, err)
		}
	}

	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache did not sync")
	}

	// A direct (non-cached) client for the churn writes, so mutations hit the
	// apiserver immediately rather than being served from the manager's cache.
	//
	// The QPS/Burst raise is load-bearing, not cosmetic: envtest hands back a
	// config capped at 1000 QPS, and an ordinary update costs two requests (a Get
	// and an Update), so the massive profile's 500 mutations/sec would sit exactly
	// on the client's own rate limiter. The published number would then describe
	// the generator's client-side throttle rather than the write path — which is
	// the most likely explanation for the ~550 records/sec plateau Task 0.8
	// recorded as "the envtest apiserver's ceiling".
	writeCfg := rest.CopyConfig(cfg)
	writeCfg.QPS = float32(max(8*profile.Rate, 4000))
	writeCfg.Burst = int(writeCfg.QPS) * 2
	writeClient, err := client.New(writeCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("create write client: %v", err)
	}

	t.Logf("load profile: %s", profile)
	t.Logf("description: %s", profile.Description)

	// --- create phase ---
	objects := planObjects(kinds, profile.Objects)
	createStart := time.Now()
	createObjects(ctx, t, writeClient, objects, profile)
	t.Logf("created %d objects across %d kind(s) in %s", len(objects), len(kinds), time.Since(createStart).Round(time.Millisecond))

	// Let the create phase's own records settle *before* the measurement window
	// opens. Without this, 20,000 in-flight Added rows would land inside the churn
	// window and inflate its records/sec by an order of magnitude — the profile's
	// sustained rate has to describe sustained churn, not a backlog draining.
	drainUntilQuiescent(metrics, 3*time.Minute)

	// --- sustained churn phase ---
	queueSampler := newQueueSampler(pipe)
	queueSampler.start()
	okStart, _ := metrics.settled()
	blockSamplesStart := metrics.sampleCount()
	cpuStart := processCPU()
	start := time.Now()
	runChurn(ctx, t, writeClient, objects, profile)
	churnElapsed := time.Since(start)
	cpuChurn := processCPU().sub(cpuStart)

	// The alloc profile is cumulative from process start, so a pre-churn snapshot
	// is what makes the churn window itself analyzable:
	//   go tool pprof -base allocs-prechurn.pb.gz allocs.pb.gz
	if *flagPprofDir != "" {
		if err := writeProfile(*flagPprofDir, "allocs", "allocs-prechurn.pb.gz"); err != nil {
			t.Errorf("write pre-drain alloc profile: %v", err)
		}
	}

	// --- drain: let the async pipeline settle the writes it already accepted so
	// the throughput figure reflects records actually persisted, not just
	// enqueued. Poll until the settled count stops moving (or a safety cap).
	drainUntilQuiescent(metrics, 2*time.Minute)
	queueSampler.stop()

	okEnd, failedEnd := metrics.settled()
	settled := okEnd - okStart
	p50, p99 := metrics.percentiles(blockSamplesStart)
	recordsPerSec := float64(settled) / churnElapsed.Seconds()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	report := formatReport(profile, runReport{
		settled:        settled,
		failed:         failedEnd,
		churnElapsed:   churnElapsed,
		recordsPerSec:  recordsPerSec,
		enqueueP50:     p50,
		enqueueP99:     p99,
		peakWriteQueue: metrics.peakDepth(),
		peakPipeQueue:  queueSampler.peak(),
		cpu:            cpuChurn,
		peakRSSBytes:   peakRSSBytes(),
		heapInUse:      mem.HeapInuse,
		sys:            mem.Sys,
		cacheEntries:   gatherGauge(t, registry, "kuberecord_hashcache_entries"),
		dedupSkips:     gatherCounter(t, registry, "kuberecord_dedup_skips_total"),
		dropped:        gatherCounter(t, registry, "kuberecord_dropped_total"),
	})

	// Report to stdout so `make bench-load` surfaces it directly.
	fmt.Print(report)

	if *flagPprofDir != "" {
		if err := writeRunProfiles(*flagPprofDir, report); err != nil {
			t.Errorf("write pprof profiles: %v", err)
		} else {
			t.Logf("wrote heap/alloc profiles and summary to %s", *flagPprofDir)
		}
	}

	// Shut the manager down cleanly (drains the writer, closes the connection).
	cancel()
	select {
	case err := <-mgrDone:
		if err != nil {
			t.Logf("manager stopped with: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Log("manager did not stop within 60s")
	}

	if settled == 0 {
		t.Fatal("no records settled — the pipeline produced nothing; check ClickHouse connectivity")
	}
	if failedEnd > 0 {
		t.Errorf("%d writes settled as failed; the envelope this run reports is not a healthy-backend one", failedEnd)
	}

	// --- the profile's own pass criteria ---
	if min := profile.Pass.MinRecordsPerSec; min > 0 && recordsPerSec < min {
		t.Errorf("profile %s requires ≥%.0f records/sec sustained, measured %.0f",
			profile.Name, min, recordsPerSec)
	}
	if maxMs := profile.Pass.MaxEnqueueBlockP99Ms; maxMs > 0 && p99*1000 > maxMs {
		t.Errorf("profile %s requires p99 enqueue-block <%.1f ms, measured %.3f ms",
			profile.Name, maxMs, p99*1000)
	}
}

// gvksByKind indexes the run's kinds the way pipeline keys arrive: by Kind name.
func gvksByKind(kinds []churnKind) map[string]schema.GroupVersionKind {
	gvks := make(map[string]schema.GroupVersionKind, len(kinds))
	for _, kind := range kinds {
		gvks[kind.GVK.Kind] = kind.GVK
	}
	return gvks
}

// watchKind registers one kind's informer handler, translating every event into a
// work key exactly as the WatchManager's fan-out does — Group and Kind come from
// the *interest* (here, the kind table), never from the object, so a tombstone
// keys identically to the live object it replaced (Invariant 7).
func watchKind(ctx context.Context, mgr ctrl.Manager, pipe *pipeline.Pipeline, kind churnKind, t *testing.T) error {
	probe := &unstructured.Unstructured{}
	probe.SetGroupVersionKind(kind.GVK)
	informer, err := mgr.GetCache().GetInformer(ctx, probe)
	if err != nil {
		return fmt.Errorf("get informer: %w", err)
	}

	enqueue := func(obj any) {
		key, keyErr := toolscache.DeletionHandlingMetaNamespaceKeyFunc(obj)
		if keyErr != nil {
			t.Logf("derive key: %v", keyErr)
			return
		}
		namespace, name, splitErr := toolscache.SplitMetaNamespaceKey(key)
		if splitErr != nil {
			t.Logf("split key %q: %v", key, splitErr)
			return
		}
		pipe.Add(pipeline.Key{
			Sink:      loadgenSink,
			Group:     kind.GVK.Group,
			Kind:      kind.GVK.Kind,
			Namespace: namespace,
			Name:      name,
		})
	}
	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		UpdateFunc: func(_, obj any) { enqueue(obj) },
		DeleteFunc: enqueue,
	}); err != nil {
		return fmt.Errorf("add event handler: %w", err)
	}
	return nil
}

// createObjects builds the profile's whole object set before the sustain phase.
//
// It runs the creates in parallel across the profile's worker count: at 20,000
// objects a serial loop spends minutes on apiserver round-trip latency alone,
// which is time the measured window never sees but a developer waiting on
// `make bench-load` certainly does.
func createObjects(ctx context.Context, t *testing.T, c client.Client, objects []churnTarget, p Profile) {
	t.Helper()

	work := make(chan churnTarget)
	var wg sync.WaitGroup
	for range p.Concurrency {
		wg.Go(func() {
			for target := range work {
				obj := target.kind.build(churnNamespace, target.name, 0, p.PayloadBytes)
				if err := c.Create(ctx, obj); err != nil {
					t.Errorf("create %s %s: %v", target.kind.GVK.Kind, target.name, err)
				}
			}
		})
	}
	for _, target := range objects {
		work <- target
	}
	close(work)
	wg.Wait()
}

// runChurn sustains p.Rate mutations per second for p.Duration. A single ticker
// paces the target rate and dispatches each mutation to a pool of p.Concurrency
// workers, so the many small apiserver round-trips overlap rather than
// serializing — without that, a single-threaded generator caps out well below
// the write path's real ceiling (the write path is what this harness exists to
// measure). Objects are partitioned across workers so no two workers ever mutate
// the same object concurrently, avoiding spurious 409 conflicts that would
// otherwise depress the achieved rate for reasons unrelated to the sink.
//
// If every worker is busy when a tick fires, that tick is dropped and counted:
// the achieved records/sec then honestly reflects the ceiling of this environment
// rather than silently pretending the target rate was applied.
func runChurn(ctx context.Context, t *testing.T, c client.Client, objects []churnTarget, p Profile) {
	t.Helper()
	workers := max(p.Concurrency, 1)

	// jobs carries a monotonically-increasing revision so every mutation changes
	// its object's content hash (a Modified event, never a dedup skip).
	jobs := make(chan int, workers)
	var wg sync.WaitGroup
	for w := range workers {
		// Partition the object pool: worker w owns objects[w], objects[w+workers], …
		var owned []churnTarget
		for i := w; i < len(objects); i += workers {
			owned = append(owned, objects[i])
		}
		if len(owned) == 0 {
			continue
		}
		wg.Go(func() {
			churnWorker(ctx, t, c, owned, p, int64(w), jobs)
		})
	}

	interval := time.Second / time.Duration(p.Rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	deadline := time.After(p.Duration.Duration())

	dispatched, dropped := 0, 0
	revision := 1
dispatch:
	for {
		select {
		case <-ctx.Done():
			break dispatch
		case <-deadline:
			break dispatch
		case <-ticker.C:
			select {
			case jobs <- revision:
				revision++
				dispatched++
			default:
				// All workers busy — this tick is dropped (see doc comment).
				dropped++
			}
		}
	}
	close(jobs)
	wg.Wait()

	t.Logf("churn dispatched %d mutations, dropped %d ticks (all workers busy)", dispatched, dropped)
	if dropped > dispatched/100 {
		t.Logf("WARNING: %.1f%% of ticks were dropped — the generator, not the write path, "+
			"is the limit at this rate", 100*float64(dropped)/float64(dispatched+dropped))
	}
}

// churnWorker performs one mutation per revision received on jobs, against its
// own partition of the object pool (so concurrent workers never collide on a
// name). A p.DeleteRatio fraction of mutations delete-and-recreate the object
// (exercising the deletion + reincarnation paths); the rest are updates. The rng
// is seeded per worker so a run is reproducible.
func churnWorker(ctx context.Context, t *testing.T, c client.Client, owned []churnTarget,
	p Profile, seed int64, jobs <-chan int) {
	rng := rand.New(rand.NewSource(seed + 1))
	for revision := range jobs {
		target := owned[rng.Intn(len(owned))]
		if p.DeleteRatio > 0 && rng.Float64() < p.DeleteRatio {
			// Delete then recreate under the same name (fresh UID): a Deleted
			// record plus a subsequent Added for the reincarnation.
			gone := &unstructured.Unstructured{}
			gone.SetGroupVersionKind(target.kind.GVK)
			gone.SetNamespace(churnNamespace)
			gone.SetName(target.name)
			if err := c.Delete(ctx, gone); err != nil {
				t.Logf("churn delete %s %s: %v", target.kind.GVK.Kind, target.name, err)
				continue
			}
			if err := c.Create(ctx, target.kind.build(churnNamespace, target.name, revision, p.PayloadBytes)); err != nil {
				t.Logf("churn recreate %s %s: %v", target.kind.GVK.Kind, target.name, err)
			}
			continue
		}
		// Ordinary update: fetch, bump the revision, persist.
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(target.kind.GVK)
		if err := c.Get(ctx, client.ObjectKey{Namespace: churnNamespace, Name: target.name}, obj); err != nil {
			t.Logf("churn get %s %s: %v", target.kind.GVK.Kind, target.name, err)
			continue
		}
		setRevision(obj, revision)
		if err := c.Update(ctx, obj); err != nil {
			t.Logf("churn update %s %s: %v", target.kind.GVK.Kind, target.name, err)
		}
	}
}

// queueSampler tracks the peak depth of the pipeline's own workqueue, which is
// the backlog figure that says whether the data plane kept up with the churn:
// the hand-off queue depth only describes the last hop, so a pipeline that is
// falling behind at the *workqueue* would otherwise be invisible in the report.
type queueSampler struct {
	pipe *pipeline.Pipeline
	done chan struct{}
	wg   sync.WaitGroup

	mu   sync.Mutex
	high int
}

func newQueueSampler(pipe *pipeline.Pipeline) *queueSampler {
	return &queueSampler{pipe: pipe, done: make(chan struct{})}
}

// start begins sampling. The 100 ms interval is a deliberate compromise: often
// enough to catch a transient backlog at 500 mutations/sec, rarely enough that
// the sampler itself is not part of what the run measures.
func (s *queueSampler) start() {
	s.wg.Go(func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				depth := s.pipe.QueueLen()
				s.mu.Lock()
				if depth > s.high {
					s.high = depth
				}
				s.mu.Unlock()
			}
		}
	})
}

func (s *queueSampler) stop() {
	close(s.done)
	s.wg.Wait()
}

func (s *queueSampler) peak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.high
}

// runReport is every measured figure for one run, so formatting stays separate
// from measurement (the same struct feeds stdout and the committed summary).
type runReport struct {
	settled        int64
	failed         int64
	churnElapsed   time.Duration
	recordsPerSec  float64
	enqueueP50     float64
	enqueueP99     float64
	peakWriteQueue float64
	peakPipeQueue  int
	cpu            cpuTime
	peakRSSBytes   int64
	heapInUse      uint64
	sys            uint64
	cacheEntries   float64
	dedupSkips     float64
	dropped        float64
}

// formatReport renders the run as the block `make bench-load` prints and
// docs/PERFORMANCE.md quotes. Hardware is part of the report because an envelope
// without it is not a claim about anything (Task 2.3 AC).
func formatReport(p Profile, r runReport) string {
	var b strings.Builder
	cores := r.cpu.cores(r.churnElapsed)
	fmt.Fprintf(&b, "\n===== kuberecord load harness result =====\n")
	fmt.Fprintf(&b, "profile               : %s\n", p)
	fmt.Fprintf(&b, "hardware              : %s/%s, %d logical CPUs, Go %s\n",
		runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintf(&b, "sustained records/sec : %.0f  (%d records settled over %.1fs churn)\n",
		r.recordsPerSec, r.settled, r.churnElapsed.Seconds())
	fmt.Fprintf(&b, "enqueue-block p50     : %.3f ms\n", r.enqueueP50*1000)
	fmt.Fprintf(&b, "enqueue-block p99     : %.3f ms\n", r.enqueueP99*1000)
	fmt.Fprintf(&b, "peak write_queue_depth: %.0f\n", r.peakWriteQueue)
	fmt.Fprintf(&b, "peak pipeline backlog : %d work items\n", r.peakPipeQueue)
	fmt.Fprintf(&b, "CPU over churn        : %.2f cores (user %.1fs + sys %.1fs over %.1fs wall)\n",
		cores, r.cpu.user.Seconds(), r.cpu.system.Seconds(), r.churnElapsed.Seconds())
	fmt.Fprintf(&b, "peak process RSS      : %.1f MiB\n", mib(float64(r.peakRSSBytes)))
	fmt.Fprintf(&b, "Go heap in use        : %.1f MiB\n", mib(float64(r.heapInUse)))
	fmt.Fprintf(&b, "Go runtime Sys        : %.1f MiB\n", mib(float64(r.sys)))
	fmt.Fprintf(&b, "hashCache entries     : %.0f\n", r.cacheEntries)
	fmt.Fprintf(&b, "dedup skips / dropped : %.0f / %.0f\n", r.dedupSkips, r.dropped)
	if r.failed > 0 {
		fmt.Fprintf(&b, "WARNING: %d writes settled as failed\n", r.failed)
	}
	fmt.Fprintf(&b, "==========================================\n\n")
	return b.String()
}

func mib(bytes float64) float64 { return bytes / (1024 * 1024) }

// cpuTime is a process CPU-time reading, split so a report can say whether the
// cost was the operator's own work (user) or the kernel's on its behalf (sys).
type cpuTime struct {
	user   time.Duration
	system time.Duration
}

// sub returns the CPU consumed between an earlier reading and this one.
func (c cpuTime) sub(earlier cpuTime) cpuTime {
	return cpuTime{user: c.user - earlier.user, system: c.system - earlier.system}
}

// cores converts CPU time into "cores busy" over a wall-clock window, which is
// the figure that scales across machines with different core counts.
func (c cpuTime) cores(wall time.Duration) float64 {
	if wall <= 0 {
		return 0
	}
	return (c.user + c.system).Seconds() / wall.Seconds()
}

// processCPU reads this process's consumed CPU time via getrusage(RUSAGE_SELF).
//
// It measures the whole test process — pipeline, informers *and* the churn
// generator — so it is an upper bound on the operator's own CPU, not an
// attribution. The report says so; a per-goroutine attribution would need the
// operator in its own process, which is what the e2e and chaos suites provide.
func processCPU() cpuTime {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return cpuTime{}
	}
	return cpuTime{
		user:   time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond,
		system: time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond,
	}
}

// peakRSSBytes returns the process's peak resident set size in bytes via
// getrusage(RUSAGE_SELF). ru_maxrss is reported in bytes on darwin and in
// kilobytes on linux, so the unit is normalized by GOOS.
func peakRSSBytes() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	maxrss := int64(ru.Maxrss)
	if runtime.GOOS == "linux" {
		return maxrss * 1024
	}
	return maxrss
}

// writeRunProfiles commits the heap and alloc profiles plus the run's own summary
// into dir, which is what docs/perf/ holds for the before/after comparison.
func writeRunProfiles(dir, summary string) error {
	// A GC first, so the heap profile describes live memory rather than whatever
	// had not been collected yet when the run finished.
	runtime.GC()
	if err := writeProfile(dir, "heap", "heap.pb.gz"); err != nil {
		return err
	}
	if err := writeProfile(dir, "allocs", "allocs.pb.gz"); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.txt"), []byte(summary), 0o600); err != nil {
		return fmt.Errorf("write run summary: %w", err)
	}
	return nil
}

// writeProfile writes one named runtime profile into dir as gzipped protobuf,
// the format `go tool pprof` reads directly.
func writeProfile(dir, profileName, fileName string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create profile directory %s: %w", dir, err)
	}
	profile := pprof.Lookup(profileName)
	if profile == nil {
		return fmt.Errorf("no runtime profile named %q", profileName)
	}
	path := filepath.Join(dir, fileName)
	f, err := os.Create(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	if err := profile.WriteTo(f, 0); err != nil {
		if closeErr := f.Close(); closeErr != nil {
			return fmt.Errorf("write %s: %w (and close: %v)", path, err, closeErr)
		}
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}
	return nil
}

// gatherGauge reads one gauge's value out of the run's private registry. It sums
// across label values, which is exactly right for the single-sink harness and
// keeps the caller from having to know a metric's label set.
func gatherGauge(t *testing.T, reg *prometheus.Registry, name string) float64 {
	return gatherSum(t, reg, name, func(m *dto.Metric) float64 {
		if m.Gauge == nil {
			return 0
		}
		return m.Gauge.GetValue()
	})
}

// gatherCounter reads one counter's total out of the run's private registry.
func gatherCounter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	return gatherSum(t, reg, name, func(m *dto.Metric) float64 {
		if m.Counter == nil {
			return 0
		}
		return m.Counter.GetValue()
	})
}

func gatherSum(t *testing.T, reg *prometheus.Registry, name string, value func(*dto.Metric) float64) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Logf("gather %s: %v", name, err)
		return 0
	}
	total := 0.0
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			total += value(metric)
		}
	}
	return total
}

// drainUntilQuiescent waits until the settled-write count has been stable for a
// short window, or until timeout elapses, so throughput accounting includes the
// writes the async pipeline had already accepted when churn stopped.
func drainUntilQuiescent(m *harnessMetrics, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	prevOK, _ := m.settled()
	stableFor := time.Duration(0)
	const stableTarget = 2 * time.Second
	for time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
		ok, _ := m.settled()
		if ok == prevOK {
			stableFor += 250 * time.Millisecond
			if stableFor >= stableTarget {
				return
			}
			continue
		}
		prevOK = ok
		stableFor = 0
	}
}

// cleanupClickHouse drops the throwaway schema-v1 tables. It opens its own
// short-lived connection because the harness's CHWriter connection is closed by
// the manager's own shutdown sequence.
func cleanupClickHouse(cfg clickhouse.Config) {
	conn, err := chdriver.Open(&chdriver.Options{
		Addr:        []string{cfg.Addr},
		Auth:        chdriver.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
		Protocol:    chdriver.Native,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second,
	})
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = conn.Exec(dropCtx, "DROP TABLE IF EXISTS resource_states")
	_ = conn.Exec(dropCtx, "DROP TABLE IF EXISTS watch_scopes")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
