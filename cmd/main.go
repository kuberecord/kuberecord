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
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/controller"
	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/plan"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/sink/clickhouse"
	"github.com/yelzhy/kuberecord/internal/sink/s3"
	"github.com/yelzhy/kuberecord/internal/sink/s3/awsstore"
	"github.com/yelzhy/kuberecord/internal/watch"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	// +kubebuilder:scaffold:scheme
}

// Connection-timeout fallbacks for a ClickHouseSink whose spec omits them.
//
// The CRD carries the same values as field defaults, so these are only reached
// for an object that predates the defaults or was written by a client that
// stripped them. They are spelled here rather than left as Go zero values
// because a zero dial timeout means "wait forever", which is the one behavior a
// sink pointed at an unreachable address must not have.
const (
	defaultSinkDialTimeout = 5 * time.Second
	defaultSinkReadTimeout = 10 * time.Second
)

// getEnvOrDefault returns the value of the named environment variable, or
// def if it is unset. Used to let flags fall back to env vars (e.g. for
// ConfigMap/Secret-projected settings) while keeping flag overrides working.
func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// getEnvDurationOrDefault is getEnvOrDefault for time.Duration flags. An
// unparsable value falls back to def rather than failing startup.
//
// This runs as a flag default-value expression, evaluated before
// flag.Parse()/ctrl.SetLogger() in main() — setupLog isn't wired to a real
// sink yet at this point, so a warning logged through it here would be
// silently discarded. fmt.Fprintf to stderr is used instead so a
// misconfigured env var is actually visible.
func getEnvDurationOrDefault(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kuberecord: invalid duration %q for env var %s, using default %s: %v\n", v, key, def, err)
		return def
	}
	return d
}

// getEnvIntOrDefault is getEnvOrDefault for int flags. An unparsable value
// falls back to def rather than failing startup. See getEnvDurationOrDefault
// for why this logs via stderr rather than setupLog.
func getEnvIntOrDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kuberecord: invalid integer %q for env var %s, using default %d: %v\n", v, key, def, err)
		return def
	}
	return n
}

// getEnvBoolOrDefault is getEnvOrDefault for bool flags. An unparsable value
// falls back to def rather than failing startup. See getEnvDurationOrDefault
// for why this logs via stderr rather than setupLog.
func getEnvBoolOrDefault(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kuberecord: invalid boolean %q for env var %s, using default %t: %v\n", v, key, def, err)
		return def
	}
	return b
}

// writerTuning holds the async-write-path knobs D2 requires operators to size
// per environment: queue capacity, worker count, the two client-side batching
// knobs, the enqueue backpressure timeout, and the shutdown drain budget.
//
// Since Task 1.10 these are *fallbacks*, not the configuration itself: every
// knob is a per-sink field on `ClickHouseSink.spec.writer`, and these values
// apply only where a sink leaves one unset. That keeps a fleet-wide default
// expressible on the Deployment while a single busy sink can still be tuned on
// its own CR. It is a distinct struct (rather than fields sprinkled through
// main) so registerWriterFlags can be exercised in isolation by cmd/main_test.go.
type writerTuning struct {
	queueSize      int
	workers        int
	batchMaxRows   int
	batchMaxWait   time.Duration
	enqueueTimeout time.Duration
	drainTimeout   time.Duration
}

// registerWriterFlags registers the six --writer-* flags (and their WRITER_*
// env twins) on fs and returns the struct they bind into, following the same
// getEnvOrDefault flag/env dual-sourcing pattern as every other setting: a flag
// wins if given, otherwise the env var, otherwise the shipped default. The
// defaults are the exported clickhouse.Default* constants, so the operator's
// out-of-the-box behavior, its --help text, and NewCHWriter's zero-value
// fallback can never drift apart. An unparsable env value falls back to the
// default with the existing stderr warning (see getEnvIntOrDefault).
//
// Split out of main() so cmd/main_test.go can drive it against a fresh
// flag.FlagSet and assert flag parsing, env fallback, and invalid-value
// degradation without touching the global flag.CommandLine.
func registerWriterFlags(fs *flag.FlagSet) *writerTuning {
	// Resolve each env-twin default first so the flag registrations below stay
	// within the line-length limit and read as a clean flag/default/help table.
	var (
		queueSizeDef      = getEnvIntOrDefault("WRITER_QUEUE_SIZE", clickhouse.DefaultWriteQueueSize)
		workersDef        = getEnvIntOrDefault("WRITER_WORKERS", clickhouse.DefaultWriteWorkers)
		batchMaxRowsDef   = getEnvIntOrDefault("WRITER_BATCH_MAX_ROWS", clickhouse.DefaultBatchMaxRows)
		batchMaxWaitDef   = getEnvDurationOrDefault("WRITER_BATCH_MAX_WAIT", clickhouse.DefaultBatchMaxWait)
		enqueueTimeoutDef = getEnvDurationOrDefault("WRITER_ENQUEUE_TIMEOUT", clickhouse.DefaultEnqueueTimeout)
		drainTimeoutDef   = getEnvDurationOrDefault("WRITER_DRAIN_TIMEOUT", clickhouse.DefaultShutdownDrainTimeout)
	)
	t := &writerTuning{}
	fs.IntVar(&t.queueSize, "writer-queue-size", queueSizeDef,
		"Fallback capacity of a sink's async write hand-off queue (jobs), used when the ClickHouseSink omits "+
			"spec.writer.queueSize. Can also be set via the WRITER_QUEUE_SIZE env var.")
	fs.IntVar(&t.workers, "writer-workers", workersDef,
		"Fallback number of workers draining a sink's write queue, used when the ClickHouseSink omits "+
			"spec.writer.workers. Can also be set via the WRITER_WORKERS env var.")
	fs.IntVar(&t.batchMaxRows, "writer-batch-max-rows", batchMaxRowsDef,
		"Fallback row count at which a worker flushes its accumulated insert batch, used when the ClickHouseSink "+
			"omits spec.writer.batchMaxRows. Can also be set via the WRITER_BATCH_MAX_ROWS env var.")
	fs.DurationVar(&t.batchMaxWait, "writer-batch-max-wait", batchMaxWaitDef,
		"Fallback maximum time a batch's first job waits for the batch to fill, used when the ClickHouseSink omits "+
			"spec.writer.batchMaxWait. Can also be set via the WRITER_BATCH_MAX_WAIT env var.")
	fs.DurationVar(&t.enqueueTimeout, "writer-enqueue-timeout", enqueueTimeoutDef,
		"Fallback time Enqueue waits for queue room before returning an error, used when the ClickHouseSink omits "+
			"spec.writer.enqueueTimeout. Can also be set via the WRITER_ENQUEUE_TIMEOUT env var.")
	fs.DurationVar(&t.drainTimeout, "writer-drain-timeout", drainTimeoutDef,
		"Fallback budget for draining a sink's queued writes during shutdown, used when the ClickHouseSink omits "+
			"spec.writer.drainTimeout. Can also be set via the WRITER_DRAIN_TIMEOUT env var.")
	return t
}

// operatorConfig is everything the composition root needs that is not derivable
// from the manager itself: the operator-level identity and tuning the flags
// resolved.
//
// It exists so setupOperator takes no dependency on the global flag set, which
// is what lets cmd's envtest boot the real graph — every runnable, every
// reconciler — without a process to configure.
type operatorConfig struct {
	// clusterID stamps every row and every scope-epoch event this operator
	// writes. It is a property of the operator instance rather than of a sink
	// (one operator serves one cluster; a cluster may have several sinks), which
	// is why it survives as a flag while the ClickHouse connection settings did
	// not.
	clusterID string

	// operatorNamespace is where a ClickHouseSink's credentialsSecretRef is
	// looked up when it omits a namespace. It is explicit configuration because
	// that default is a security boundary: the operator holds Secret read rights
	// in this namespace and nowhere else (Task 1.9).
	operatorNamespace string

	// pipelineWorkers is the number of goroutines draining the shared work
	// queue. Zero means pipeline.DefaultWorkers.
	pipelineWorkers int

	// autoCreateSchema makes every sink instance apply the shipped DDL
	// idempotently before it starts writing. It is operator-level rather than
	// per-sink on purpose: "may this operator run DDL?" is a deployment-time
	// privilege decision, not something the author of a sink CR should grant
	// themselves.
	autoCreateSchema bool

	// writer holds the per-sink write-path fallbacks (see writerTuning).
	writer writerTuning
}

// dataPlane is the composition root's answer to the one genuine cycle in the
// object graph.
//
// The pipeline needs the WatchManager (as its ListerRegistry) and the
// WatchManager needs the pipeline (as its work-key sink); the sink runtime needs
// the pipeline (to evict a deleted sink's dedup state) but the pipeline needs the
// sink runtime (to route writes). No construction order satisfies all four, so
// exactly one indirection is unavoidable — and it lives here, in the wiring,
// rather than as a nil-tolerating field inside a package that would then have to
// defend against it on every call.
//
// Binding happens before mgr.Start, on this goroutine, and every reader runs on a
// goroutine the manager creates afterwards — so the writes are visible without
// synchronization. The nil guards below are therefore not a race defense but an
// Invariant-5 one: a future wiring mistake degrades to a retry instead of a
// nil-pointer panic inside a worker.
type dataPlane struct {
	pipe    *pipeline.Pipeline
	watches *watch.WatchManager
	warm    *pipeline.WarmCoordinator
}

// errDataPlaneUnbound reports a lookup that arrived before the wiring completed.
// It is retryable by construction — the pipeline re-queues the key — so it costs
// a delay rather than a lost record.
var errDataPlaneUnbound = errors.New("data plane is not wired up yet")

// unsettled is what Settled reports before binding: a channel that is never
// closed, so boot reconciliation waits rather than running against a desired
// state nothing has populated. Closing early would let it read "no rule wants
// this scope" and write a Stopped row for every live scope — the exact audit lie
// scope epochs exist to prevent.
var unsettled = make(chan struct{})

// bind completes the graph. It is called once, after every component exists and
// before the manager starts any of them.
func (d *dataPlane) bind(pipe *pipeline.Pipeline, watches *watch.WatchManager, warm *pipeline.WarmCoordinator) {
	d.pipe = pipe
	d.watches = watches
	d.warm = warm
}

// RemoveSink implements sink.Pipeline.
func (d *dataPlane) RemoveSink(id sink.ID) {
	if d.pipe == nil {
		setupLog.Error(errDataPlaneUnbound, "Cannot evict a deleted sink's pipeline state",
			"sink", id.String())
		return
	}
	d.pipe.RemoveSink(id)
}

// ForgetSink implements sink.WarmHooks. The coordinator is reached through this
// indirection rather than handed to the sink runtime directly because the runtime
// is constructed first — it is the pipeline's router, and the pipeline is the
// coordinator's own dependency.
func (d *dataPlane) ForgetSink(id sink.ID) {
	if d.warm == nil {
		setupLog.Error(errDataPlaneUnbound, "Cannot clear a deleted sink's warm bookkeeping",
			"sink", id.String())
		return
	}
	d.warm.ForgetSink(id)
}

// Get implements pipeline.ListerRegistry.
func (d *dataPlane) Get(ref pipeline.Key) (*unstructured.Unstructured, bool, bool, error) {
	if d.watches == nil {
		return nil, false, false, errDataPlaneUnbound
	}
	return d.watches.Get(ref)
}

// RedactionFor implements pipeline.RedactionRegistry.
//
// Unbound reports "no policy", which makes the pipeline retry rather than write.
// It is unreachable in practice — Get is consulted first on every work item and
// fails the same lookup with errDataPlaneUnbound — but the direction matters
// more than the reachability: an unwired data plane must never be the reason an
// object is written with less redaction than its rules asked for.
func (d *dataPlane) RedactionFor(ref pipeline.Key) (*pipeline.RedactionPolicy, bool) {
	if d.watches == nil {
		setupLog.Error(errDataPlaneUnbound, "Cannot resolve a redaction policy", "key", ref.String())
		return nil, false
	}
	return d.watches.RedactionFor(ref)
}

// ScopeSynced implements pipeline.ScopeStates.
func (d *dataPlane) ScopeSynced(id sink.ID, scope pipeline.ScopeKey) bool {
	if d.watches == nil {
		return false
	}
	return d.watches.ScopeSynced(id, scope)
}

// ScopeDesired implements pipeline.ScopeStates.
func (d *dataPlane) ScopeDesired(id sink.ID, scope pipeline.ScopeKey) bool {
	if d.watches == nil {
		return false
	}
	return d.watches.ScopeDesired(id, scope)
}

// Settled implements pipeline.ScopeStates.
func (d *dataPlane) Settled() <-chan struct{} {
	if d.watches == nil {
		return unsettled
	}
	return d.watches.Settled()
}

// operator is the assembled process: the components the two setup halves share,
// plus the parking bridge the sink runtime calls back into.
type operator struct {
	registry *plan.Registry
	sinks    *sink.SinkManager
	plane    *dataPlane

	// parker is created last, because a rule reconciler's park channel only
	// exists once it has been registered with the manager. Until then parkRules
	// has nothing to deliver to — a state that cannot outlive setupOperator,
	// since a failed registration aborts the process.
	parker *controller.Parker
}

// setupOperator is kuberecord's composition root: it builds the data plane, the
// control plane and the health probes on mgr, and adds every runnable to it.
//
// It deliberately starts nothing. The manager owns every lifecycle, which is
// what gives shutdown its ordering (workqueue drains, then sinks flush) and what
// lets this whole graph be constructed in a test against envtest with no CRs at
// all — the state the operator ships in and must be healthy in.
func setupOperator(mgr ctrl.Manager, cfg operatorConfig) error {
	op := &operator{plane: &dataPlane{}}
	if err := op.setupDataPlane(mgr, cfg); err != nil {
		return err
	}
	if err := op.setupControlPlane(mgr, cfg); err != nil {
		return err
	}

	// Both probes are plain pings, and readyz deliberately so: an operator with
	// no ClickHouseSink at all is a valid, healthy state (it is the state a fresh
	// install boots into), and a sink that is unreachable is reported as a
	// condition on its own CR — not as process unreadiness that would take the
	// pod out of service and stop every other sink with it.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("set up the health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("set up the ready check: %w", err)
	}
	return nil
}

// setupDataPlane builds the desired-state registry, the sink runtime, the
// pipeline, the warm/GC coordinator, the scope-epoch recorder and the watch
// manager, and registers all five runnables with mgr.
//
// The order is forced by the dependencies, with one indirection (see dataPlane):
// the registry answers "which rules use this sink?", so it precedes the sink
// runtime; the sink runtime is the pipeline's router, so it precedes the
// pipeline; the recorder drives the coordinator, so the coordinator precedes it;
// and the watch manager, which everything else is level-triggered from, comes
// last because it is the one component the others can only be handed through the
// binding.
func (op *operator) setupDataPlane(mgr ctrl.Manager, cfg operatorConfig) error {
	metrics := pipeline.PipelineMetricsInstance()
	op.registry = plan.New()

	sinks, err := sink.NewSinkManager(sink.ManagerOptions{
		Factory:    newSinkFactory(metrics),
		Pipeline:   op.plane,
		Warm:       op.plane,
		Dependents: op.registry,
		OnSinkGone: op.parkRules,
	})
	if err != nil {
		return fmt.Errorf("build the sink runtime: %w", err)
	}
	op.sinks = sinks

	pipe, err := pipeline.New(pipeline.Options{
		ClusterID:  cfg.clusterID,
		Workers:    cfg.pipelineWorkers,
		Lister:     op.plane,
		Router:     sinks,
		Redactions: op.plane,
		Metrics:    metrics,
	})
	if err != nil {
		return fmt.Errorf("build the pipeline: %w", err)
	}

	warm, err := pipeline.NewWarmCoordinator(pipeline.WarmOptions{
		Pipeline:    pipe,
		Scopes:      op.plane,
		Readers:     sinks,
		ScopeEvents: sinks,
	})
	if err != nil {
		return fmt.Errorf("build the warm/GC coordinator: %w", err)
	}

	recorder, err := watch.NewScopeEpochRecorder(watch.ScopeRecorderOptions{
		ClusterID: cfg.clusterID,
		Events:    sinks,
		Warmer:    warm,
	})
	if err != nil {
		return fmt.Errorf("build the scope-epoch recorder: %w", err)
	}

	dyn, err := dynamic.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("build the dynamic client: %w", err)
	}
	watches, err := watch.New(watch.Options{
		Registry: op.registry,
		// The resolver is shared with the rule reconcilers so a kind that is not
		// installed yet is retried on one backoff gate rather than two.
		Resolver: watch.NewResolver(mgr.GetRESTMapper()),
		Dynamic:  dyn,
		Pipeline: pipe,
		Recorder: recorder,
	})
	if err != nil {
		return fmt.Errorf("build the watch manager: %w", err)
	}

	op.plane.bind(pipe, watches, warm)

	// Every one of these is leader-gated (the pipeline through controller-runtime's
	// default for a runnable that does not opt out, the rest explicitly), so a
	// standby replica holds no ClickHouse connection and runs no informers.
	runnables := []struct {
		name string
		r    manager.Runnable
	}{
		{"sink runtime", sinks},
		{"pipeline", pipe},
		{"warm/GC coordinator", warm},
		{"scope-epoch recorder", recorder},
		{"watch manager", watches},
	}
	for _, runnable := range runnables {
		if err := mgr.Add(runnable.r); err != nil {
			return fmt.Errorf("add the %s to the manager: %w", runnable.name, err)
		}
	}
	return nil
}

// setupControlPlane registers the three reconcilers and the parking bridge.
//
// The Parker can only be built once both rule reconcilers are registered, since
// registration is what creates the channels it delivers on — which is why the
// sink runtime is handed op.parkRules (a method) rather than the Parker itself.
func (op *operator) setupControlPlane(mgr ctrl.Manager, cfg operatorConfig) error {
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("build the authorization client: %w", err)
	}

	// One probe hub for every sink kind, because the sink runtime has one
	// probe-result channel carrying verdicts for all of them: a drainer per
	// reconciler would steal the other's results (see controller.SinkProbeHub).
	probes := controller.NewSinkProbeHub(op.sinks.ProbeResults())
	if err := probes.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("set up the sink probe hub: %w", err)
	}

	sinkReconciler := &controller.SinkReconciler{
		Client: mgr.GetClient(),
		// GetEventRecorderFor is deprecated in favour of the events/v1 recorder,
		// but the two are not interchangeable: the new EventRecorder's Eventf
		// takes a `related` object and an `action` verb the reconcilers do not
		// have, so switching is an API change to internal/controller (Task 1.7)
		// and to the events kuberecord emits, not a rename. Deferred deliberately
		// rather than smuggled into the e2e task.
		//nolint:staticcheck // SA1019: events/v1 migration is its own change.
		Recorder: mgr.GetEventRecorderFor("kuberecord-clickhousesink"),
		Sinks:    op.sinks,
		// The ClickHouse mapping lives here, in the wiring, so internal/controller
		// depends on no driver and cannot dial a backend even by accident
		// (Invariant 1).
		BuildConfig:       newSinkConfigBuilder(cfg.writer, cfg.autoCreateSchema),
		OperatorNamespace: cfg.operatorNamespace,
		Probes:            probes,
	}
	if err := sinkReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("set up the ClickHouseSink reconciler: %w", err)
	}

	s3SinkReconciler := &controller.S3SinkReconciler{
		Client: mgr.GetClient(),
		//nolint:staticcheck // SA1019: see the ClickHouseSink recorder above.
		Recorder: mgr.GetEventRecorderFor("kuberecord-s3sink"),
		Sinks:    op.sinks,
		// The object-store mapping lives here, in the wiring, for the same reason
		// the ClickHouse one does: internal/controller then depends on no client
		// and cannot reach a bucket even by accident (Invariant 1).
		BuildConfig:       newS3SinkConfigBuilder(cfg.writer),
		OperatorNamespace: cfg.operatorNamespace,
		Probes:            probes,
	}
	if err := s3SinkReconciler.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("set up the S3Sink reconciler: %w", err)
	}

	base := controller.RuleReconciler{
		Client: mgr.GetClient(),
		//nolint:staticcheck // SA1019: see the ClickHouseSink recorder above.
		Recorder: mgr.GetEventRecorderFor("kuberecord-streamrule"),
		Registry: op.registry,
		Resolver: watch.NewResolver(mgr.GetRESTMapper()),
		Access:   clientset.AuthorizationV1().SelfSubjectAccessReviews(),
		// One gauge for both rule kinds: it counts a set that spans them. Passed
		// through the shared base value so the two reconcilers below cannot end up
		// counting into two different instances.
		Metrics: controller.RuleMetricsInstance(),
	}
	namespaced := controller.NewStreamRuleReconciler(base)
	if err := namespaced.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("set up the StreamRule reconciler: %w", err)
	}
	clusterWide := controller.NewClusterStreamRuleReconciler(base)
	if err := clusterWide.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("set up the ClusterStreamRule reconciler: %w", err)
	}
	op.parker = controller.NewParker(namespaced, clusterWide)
	return nil
}

// parkRules implements sink.ParkFunc: it re-reconciles the rules that streamed to
// a sink that has gone for good, so each reports Ready=False/SinkMissing.
func (op *operator) parkRules(id sink.ID, ruleKeys []string) {
	if op.parker == nil {
		setupLog.Error(errDataPlaneUnbound, "Cannot park the rules of a sink that is gone",
			"sink", id.String())
		return
	}
	op.parker.SinkGone(id, ruleKeys)
}

// newSinkFactory returns the sink.Factory that turns a resolved configuration
// into a running ClickHouse backend.
//
// This is the single place in the process that knows ClickHouse exists at all.
// D6's future backends (PostgresSink, …) add a branch here — one more typed
// configuration, one more driver — and nothing else in the graph changes, because
// everything upstream speaks only the sink interfaces.
//
// The per-sink metrics view is resolved here because the factory is where a sink's
// identity and its backend meet; internal/sink cannot import internal/pipeline, so
// it could not label its own series.
func newSinkFactory(metrics *pipeline.PipelineMetrics) sink.Factory {
	return func(id sink.ID, cfg sink.InstanceConfig) (sink.Writer, error) {
		switch typed := cfg.(type) {
		case clickhouse.Config:
			return clickhouse.Open(typed, metrics.ForSink(id))
		case s3.SinkConfig:
			// context.Background is correct rather than convenient: building the
			// store performs no I/O (see awsstore.New) — it resolves process
			// environment and assembles a lazy credential chain — and the
			// sink.Factory contract has no context to thread precisely because the
			// SinkManager calls it inline from a reconcile (Invariant 1). The
			// first network call happens on the writer's own goroutines, under
			// their own deadlines.
			store, err := awsstore.New(context.Background(), typed.Client)
			if err != nil {
				return nil, fmt.Errorf("sink %s: build the object store: %w", id, err)
			}
			return s3.NewWriter(store, typed.Writer, metrics.ForSink(id)), nil
		default:
			return nil, fmt.Errorf("sink %s: %T is not a sink configuration this build can serve", id, cfg)
		}
	}
}

// newSinkConfigBuilder returns the controller.SinkConfigBuilder that maps a
// ClickHouseSink spec plus its resolved password onto a ClickHouse configuration.
//
// Every spec.writer field is optional, and an omitted one falls back to the
// operator-level --writer-* value rather than to the package default: an
// administrator who sized the write path for their cluster on the Deployment
// should not have to repeat it on every sink, while a sink that does state a value
// always wins. It performs no I/O — it translates a struct, and is called inline
// from a reconcile (Invariant 1).
func newSinkConfigBuilder(defaults writerTuning, autoCreateSchema bool) controller.SinkConfigBuilder {
	return func(_ string, spec v1alpha1.ClickHouseSinkSpec, password string) (sink.InstanceConfig, error) {
		return clickhouse.Config{
			Addr:                 spec.Connection.Addr,
			Database:             spec.Connection.Database,
			Username:             spec.Connection.Username,
			Password:             password,
			DialTimeout:          durationOrDefault(spec.Connection.DialTimeout, defaultSinkDialTimeout),
			ReadTimeout:          durationOrDefault(spec.Connection.ReadTimeout, defaultSinkReadTimeout),
			AutoCreateSchema:     autoCreateSchema,
			WriteQueueSize:       intOrDefault(spec.Writer.QueueSize, defaults.queueSize),
			WriteWorkers:         intOrDefault(spec.Writer.Workers, defaults.workers),
			BatchMaxRows:         intOrDefault(spec.Writer.BatchMaxRows, defaults.batchMaxRows),
			BatchMaxWait:         durationOrDefault(spec.Writer.BatchMaxWait, defaults.batchMaxWait),
			EnqueueTimeout:       durationOrDefault(spec.Writer.EnqueueTimeout, defaults.enqueueTimeout),
			ShutdownDrainTimeout: durationOrDefault(spec.Writer.DrainTimeout, defaults.drainTimeout),
			// checkpointEvery has no --writer-* twin: the CRD defaults it to
			// clickhouse.DefaultCheckpointEvery, so an omitted field is already the
			// shipped cadence, and 0 is a meaningful value ("no checkpoints for
			// this sink") that a fleet-wide fallback could only obscure.
			CheckpointEvery: intOrDefault(spec.Writer.CheckpointEvery, clickhouse.DefaultCheckpointEvery),
		}, nil
	}
}

// newS3SinkConfigBuilder returns the controller.S3SinkConfigBuilder that maps an
// S3Sink spec plus its resolved credentials onto an object-store configuration.
//
// The four writer knobs S3 shares with ClickHouse fall back to the same
// operator-level --writer-* values, deliberately: an administrator who sized the
// write path for their cluster on the Deployment should not have to repeat it per
// sink, and a sink that states a value still wins. spec.rotation has no --writer-*
// twin — an omitted field resolves to zero, which the S3 writer reads as "use the
// shipped default" — for the same reason checkpointEvery has none: the CRD already
// defaults both, so a fleet-wide fallback over them could only disagree with the
// schema.
//
// Like its ClickHouse twin it performs no I/O: it translates a struct, and is
// called inline from a reconcile (Invariant 1).
func newS3SinkConfigBuilder(defaults writerTuning) controller.S3SinkConfigBuilder {
	return func(_ string, spec v1alpha1.S3SinkSpec, creds controller.S3Credentials) (sink.InstanceConfig, error) {
		cfg := s3.SinkConfig{
			Client: s3.ClientConfig{
				Region:         spec.Region,
				Endpoint:       spec.Endpoint,
				ForcePathStyle: spec.ForcePathStyle,
				// A zero-valued credential is not a missing one: it is this sink
				// asking to authenticate from the ambient chain, which is the
				// recommended shape on a cloud provider (see
				// v1alpha1.S3CredentialsSpec).
				Credentials: s3.Credentials{
					AccessKeyID:     creds.AccessKeyID,
					SecretAccessKey: creds.SecretAccessKey,
					SessionToken:    creds.SessionToken,
				},
			},
			Writer: s3.Config{
				Bucket:         spec.Bucket,
				Prefix:         spec.Prefix,
				MaxObjectBytes: int64OrZero(spec.Rotation.MaxObjectBytes),
				MaxObjectAge:   durationOrDefault(spec.Rotation.MaxObjectAge, 0),
				QueueSize:      intOrDefault(spec.Writer.QueueSize, defaults.queueSize),
				Workers:        intOrDefault(spec.Writer.Workers, defaults.workers),
				EnqueueTimeout: durationOrDefault(spec.Writer.EnqueueTimeout, defaults.enqueueTimeout),
				DrainTimeout:   durationOrDefault(spec.Writer.DrainTimeout, defaults.drainTimeout),
			},
		}
		if lock := spec.ObjectLock; lock != nil {
			// The mode is passed through as the string S3 itself uses; the CRD's enum
			// has already established it is one of the two legal ones.
			cfg.Writer.ObjectLock = &s3.ObjectLock{Mode: string(lock.Mode), RetainDays: lock.RetainDays}
		}
		return cfg, nil
	}
}

// int64OrZero resolves an optional CRD int64 field, leaving an omitted one at zero
// — which every field it is used for reads as "use the shipped default".
func int64OrZero(n *int64) int64 {
	if n == nil {
		return 0
	}
	return *n
}

// durationOrDefault resolves an optional CRD duration field.
func durationOrDefault(d *metav1.Duration, def time.Duration) time.Duration {
	if d == nil {
		return def
	}
	return d.Duration
}

// intOrDefault resolves an optional CRD int32 field. The CRD bounds every one of
// these well inside int range, so the conversion cannot truncate.
func intOrDefault(n *int32, def int) int {
	if n == nil {
		return def
	}
	return int(*n)
}

// registerFlags registers every operator flag on fs and returns the config they
// bind into, along with the manager-level settings main() needs.
//
// Split out of main() for the same reason registerWriterFlags is: a test can
// drive the exact registration path against a throwaway FlagSet.
func registerFlags(fs *flag.FlagSet) (*operatorConfig, *managerFlags) {
	cfg := &operatorConfig{}
	mf := &managerFlags{}

	fs.StringVar(&cfg.clusterID, "cluster-id", getEnvOrDefault("CLUSTER_ID", "local-kind-cluster"),
		"Identifier for this cluster, recorded on every row and scope event this operator writes. "+
			"Can also be set via the CLUSTER_ID env var.")
	fs.StringVar(&cfg.operatorNamespace, "operator-namespace", getEnvOrDefault("POD_NAMESPACE", ""),
		"Namespace a ClickHouseSink's credentialsSecretRef defaults to, and the only namespace the operator "+
			"reads Secrets in. Can also be set via the POD_NAMESPACE env var (downward API in the shipped manifest).")
	fs.IntVar(&cfg.pipelineWorkers, "pipeline-workers",
		getEnvIntOrDefault("PIPELINE_WORKERS", pipeline.DefaultWorkers),
		"Number of workers draining the shared data-plane workqueue. Can also be set via the PIPELINE_WORKERS "+
			"env var.")
	fs.BoolVar(&cfg.autoCreateSchema, "ch-auto-create-schema", getEnvBoolOrDefault("CH_AUTO_CREATE_SCHEMA", false),
		"If set, every sink instance executes the shipped ClickHouse DDL (deploy/clickhouse/schema) idempotently "+
			"before it starts writing. Defaults to false. Can also be set via the CH_AUTO_CREATE_SCHEMA env var.")

	fs.StringVar(&mf.metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	fs.StringVar(&mf.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	fs.BoolVar(&mf.enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	fs.BoolVar(&mf.secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	fs.StringVar(&mf.webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	fs.StringVar(&mf.webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	fs.StringVar(&mf.webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	fs.StringVar(&mf.metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	fs.StringVar(&mf.metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	fs.StringVar(&mf.metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	fs.BoolVar(&mf.enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")

	cfg.writer = *registerWriterFlags(fs)
	return cfg, mf
}

// managerCacheOptions confines the manager's Secret informer to the operator's
// own namespace.
//
// Without it the operator cannot run at all under its shipped RBAC. The
// SinkReconciler watches Secrets to close the credential-rotation loop, and a
// watch through the manager's cache is a *cluster-scoped* list-and-watch by
// default — but the operator's Secret grant is a namespaced Role, on purpose
// (Task 1.9, D7: a cluster-scoped ClickHouseSink must never become a way to read
// any Secret in the cluster). The list is refused, the cache never syncs, and
// every ClickHouseSink hangs unreconciled with an empty status. It is invisible
// to envtest, whose client is effectively an administrator, and shows up the
// moment the operator runs on a real cluster.
//
// Scoping the informer — rather than widening the grant — is what keeps the
// security boundary intact: the list the operator issues is now exactly the one
// its Role permits. A sink whose credentialsSecretRef names some other namespace
// gets a cache error rather than a credential, which is the truthful outcome
// (the operator has no rights there either way) and degrades that one sink
// through CredentialsResolved=False instead of stalling the process.
//
// Only Secrets are constrained. The CRDs are cluster-scoped or watched across all
// namespaces by design, and Namespaces are listed cluster-wide for
// ClusterStreamRule selector expansion — both of which the base ClusterRole grants.
func managerCacheOptions(operatorNamespace string) cache.Options {
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Secret{}: {
				Namespaces: map[string]cache.Config{operatorNamespace: {}},
			},
		},
	}
}

// managerFlags are the controller-runtime manager's own settings: serving
// addresses, leader election and the two TLS bundles. They configure the harness
// rather than kuberecord itself, which is why they are separate from
// operatorConfig — setupOperator has no use for any of them.
type managerFlags struct {
	metricsAddr                                      string
	probeAddr                                        string
	enableLeaderElection                             bool
	secureMetrics                                    bool
	enableHTTP2                                      bool
	metricsCertPath, metricsCertName, metricsCertKey string
	webhookCertPath, webhookCertName, webhookCertKey string
}

func main() {
	cfg, mf := registerFlags(flag.CommandLine)
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	if cfg.operatorNamespace == "" {
		setupLog.Error(errNoOperatorNamespace, "Cannot start")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Cache:                  managerCacheOptions(cfg.operatorNamespace),
		Metrics:                metricsServerOptions(mf),
		WebhookServer:          webhook.NewServer(webhookServerOptions(mf)),
		HealthProbeBindAddress: mf.probeAddr,
		LeaderElection:         mf.enableLeaderElection,
		LeaderElectionID:       "885d930f.kuberecord.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	if err := setupOperator(mgr, *cfg); err != nil {
		setupLog.Error(err, "Failed to wire up the operator")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	// The operator boots idle: with no ClickHouseSink and no rules it watches
	// nothing, writes nothing and reports healthy. Streaming starts when a
	// ClickHouseSink (conventionally named "default", which is what the shipped
	// samples' spec.sink.name points at) and a StreamRule or ClusterStreamRule
	// appear — no restart, and no configuration on this process.
	setupLog.Info("Starting manager",
		"cluster_id", cfg.clusterID, "operator_namespace", cfg.operatorNamespace,
		"pipeline_workers", cfg.pipelineWorkers, "auto_create_schema", cfg.autoCreateSchema)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

// errNoOperatorNamespace reports the one setting the operator cannot guess. It is
// fatal rather than defaulted because the value is a security boundary: guessing
// it wrong would either break every sink's credential lookup or, worse, point it
// at a namespace the deployment did not intend.
var errNoOperatorNamespace = errors.New(
	"--operator-namespace (or the POD_NAMESPACE env var) must be set; it is the only namespace the operator " +
		"reads sink credentials Secrets in")

// webhookServerOptions builds the webhook server's options. No webhooks are
// registered today (D4: validation is CEL-only, with no cert-manager dependency);
// the server and its certificate flags are kept because the manager always
// constructs one.
func webhookServerOptions(mf *managerFlags) webhook.Options {
	opts := webhook.Options{TLSOpts: tlsOptions(mf)}
	if len(mf.webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", mf.webhookCertPath, "webhook-cert-name", mf.webhookCertName,
			"webhook-cert-key", mf.webhookCertKey)
		opts.CertDir = mf.webhookCertPath
		opts.CertName = mf.webhookCertName
		opts.KeyName = mf.webhookCertKey
	}
	return opts
}

// metricsServerOptions builds the metrics server's options.
//
// More info:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/server
// - https://book.kubebuilder.io/reference/metrics.html
func metricsServerOptions(mf *managerFlags) metricsserver.Options {
	opts := metricsserver.Options{
		BindAddress:   mf.metricsAddr,
		SecureServing: mf.secureMetrics,
		TLSOpts:       tlsOptions(mf),
	}
	if mf.secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/filters#WithAuthenticationAndAuthorization
		opts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	if len(mf.metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", mf.metricsCertPath, "metrics-cert-name", mf.metricsCertName,
			"metrics-cert-key", mf.metricsCertKey)
		opts.CertDir = mf.metricsCertPath
		opts.CertName = mf.metricsCertName
		opts.KeyName = mf.metricsCertKey
	}
	return opts
}

// tlsOptions disables HTTP/2 unless it was explicitly enabled.
//
// Disabling http/2 prevents being vulnerable to the HTTP/2 Stream Cancellation
// and Rapid Reset CVEs. For more information see:
// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
// - https://github.com/advisories/GHSA-4374-p667-p6c8
func tlsOptions(mf *managerFlags) []func(*tls.Config) {
	if mf.enableHTTP2 {
		return nil
	}
	return []func(*tls.Config){func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}}
}
