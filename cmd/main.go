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
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/yelzhy/kubestream/internal/pipeline"
	"github.com/yelzhy/kubestream/internal/sink/clickhouse"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	// +kubebuilder:scaffold:scheme
}

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
		fmt.Fprintf(os.Stderr, "kubestream: invalid duration %q for env var %s, using default %s: %v\n", v, key, def, err)
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
		fmt.Fprintf(os.Stderr, "kubestream: invalid integer %q for env var %s, using default %d: %v\n", v, key, def, err)
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
		fmt.Fprintf(os.Stderr, "kubestream: invalid boolean %q for env var %s, using default %t: %v\n", v, key, def, err)
		return def
	}
	return b
}

// writerTuning holds the async-write-path knobs D2 requires operators to size
// per environment: queue capacity, worker count, the two client-side batching
// knobs, the enqueue backpressure timeout, and the shutdown drain budget. It is
// a distinct struct (rather than fields sprinkled through main) so
// registerWriterFlags can be exercised in isolation by cmd/main_test.go.
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
		"Capacity of the async write hand-off queue (jobs). Can also be set via the WRITER_QUEUE_SIZE env var.")
	fs.IntVar(&t.workers, "writer-workers", workersDef,
		"Number of workers draining the write queue into ClickHouse. Can also be set via the WRITER_WORKERS env var.")
	fs.IntVar(&t.batchMaxRows, "writer-batch-max-rows", batchMaxRowsDef,
		"Row count at which a worker flushes its accumulated insert batch. "+
			"Can also be set via the WRITER_BATCH_MAX_ROWS env var.")
	fs.DurationVar(&t.batchMaxWait, "writer-batch-max-wait", batchMaxWaitDef,
		"Maximum time a batch's first job waits for the batch to fill before flushing regardless. "+
			"Can also be set via the WRITER_BATCH_MAX_WAIT env var.")
	fs.DurationVar(&t.enqueueTimeout, "writer-enqueue-timeout", enqueueTimeoutDef,
		"How long Enqueue waits for queue room before returning an error (the job is never dropped silently). "+
			"Can also be set via the WRITER_ENQUEUE_TIMEOUT env var.")
	fs.DurationVar(&t.drainTimeout, "writer-drain-timeout", drainTimeoutDef,
		"Time budget for draining queued writes to ClickHouse during graceful shutdown. "+
			"Can also be set via the WRITER_DRAIN_TIMEOUT env var.")
	return t
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	var chAddr, chDatabase, chUsername string
	var chDialTimeout, chReadTimeout time.Duration
	var chAutoCreateSchema bool
	flag.StringVar(&chAddr, "ch-addr", getEnvOrDefault("CH_ADDR", "127.0.0.1:9000"),
		"The ClickHouse server address (host:port). Can also be set via the CH_ADDR env var.")
	flag.StringVar(&chDatabase, "ch-database", getEnvOrDefault("CH_DATABASE", "kubestream"),
		"The ClickHouse database name. Can also be set via the CH_DATABASE env var.")
	flag.StringVar(&chUsername, "ch-username", getEnvOrDefault("CH_USERNAME", "default"),
		"The ClickHouse username. Can also be set via the CH_USERNAME env var.")
	flag.DurationVar(&chDialTimeout, "ch-dial-timeout", getEnvDurationOrDefault("CH_DIAL_TIMEOUT", 5*time.Second),
		"Timeout for establishing the ClickHouse connection. Can also be set via the CH_DIAL_TIMEOUT env var.")
	flag.DurationVar(&chReadTimeout, "ch-read-timeout", getEnvDurationOrDefault("CH_READ_TIMEOUT", 10*time.Second),
		"Timeout for a single ClickHouse query/insert round-trip. Can also be set via the CH_READ_TIMEOUT env var.")
	flag.BoolVar(&chAutoCreateSchema, "ch-auto-create-schema", getEnvBoolOrDefault("CH_AUTO_CREATE_SCHEMA", false),
		"If set, execute the shipped ClickHouse DDL (deploy/clickhouse/schema) idempotently at connect time. "+
			"Defaults to false. Can also be set via the CH_AUTO_CREATE_SCHEMA env var.")
	// CH_PASSWORD is intentionally env-only (no flag): flag values are
	// visible in `ps`/process listings, which a Secret-projected env var
	// avoids.
	var clusterID string
	flag.StringVar(&clusterID, "cluster-id", getEnvOrDefault("CLUSTER_ID", "local-kind-cluster"),
		"Identifier for this cluster, recorded on every row written to ClickHouse. "+
			"Can also be set via the CLUSTER_ID env var.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	// The six async-write-path knobs (D2): registered on the shared
	// flag.CommandLine so they are parsed by the flag.Parse() below.
	writer := registerWriterFlags(flag.CommandLine)
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "885d930f.kubestream.io",
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

	chConfig := clickhouse.Config{
		Addr:                 chAddr,
		Database:             chDatabase,
		Username:             chUsername,
		Password:             os.Getenv("CH_PASSWORD"),
		DialTimeout:          chDialTimeout,
		ReadTimeout:          chReadTimeout,
		AutoCreateSchema:     chAutoCreateSchema,
		WriteQueueSize:       writer.queueSize,
		WriteWorkers:         writer.workers,
		BatchMaxRows:         writer.batchMaxRows,
		BatchMaxWait:         writer.batchMaxWait,
		EnqueueTimeout:       writer.enqueueTimeout,
		ShutdownDrainTimeout: writer.drainTimeout,
	}
	if chConfig.Password == "" {
		setupLog.Info("CH_PASSWORD is not set; connecting to ClickHouse without a password")
	}

	// The ClickHouse backend owns the shared connection and implements both the
	// sink.Writer (batched inserts off the pipeline's hot path) and
	// sink.StateReader (cache warm-up) contracts; RegisterWithManager wires its
	// writer runnable, the connect-time schema-validation runnable, and the
	// schema readyz check into the manager. The data plane then depends only on
	// the sink interfaces, never on ClickHouse directly.
	chWriter, err := clickhouse.Open(chConfig, pipeline.PipelineMetricsInstance())
	if err != nil {
		setupLog.Error(err, "Failed to open ClickHouse connection")
		os.Exit(1)
	}
	if err := chWriter.RegisterWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to register ClickHouse backend with manager")
		os.Exit(1)
	}

	// The data plane is not wired yet. The per-GVK stream reconcilers were
	// replaced by internal/pipeline, and the components that feed it now exist —
	// the WatchManager (Task 1.4, answering pipeline.ListerRegistry and
	// pipeline.ScopeStates), the scope-epoch recorder and warm/GC coordinator
	// (Task 1.6) — but the SinkManager that answers pipeline.SinkRouter,
	// pipeline.StateReaderRouter and the scope-event routers (Task 1.8) does not,
	// and neither do the reconcilers that translate CRs into watch targets (Task
	// 1.7). Until Task 1.10 assembles them here, the operator starts healthy and
	// streams nothing, which is exactly the Phase 1 end state for a cluster with
	// no ClickHouseSink and no rules.
	//
	// clusterID is threaded into pipeline.Options at that point; it stays a flag
	// because it labels every row this operator writes, independent of any CR.
	setupLog.Info("Operator cluster identity", "cluster_id", clusterID)
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
