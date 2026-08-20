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
	"flag"
	"io"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yelzhy/kuberecord/api/v1alpha1"
	"github.com/yelzhy/kuberecord/internal/pipeline"
	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/sink/clickhouse"
)

// parseWriterFlags registers the --writer-* flags on a throwaway FlagSet and
// parses args against them, so a test drives the exact same registration path
// main() uses without touching the global flag.CommandLine. Parse output is
// discarded; any parse error is surfaced as a return value.
func parseWriterFlags(t *testing.T, args []string) (*writerTuning, error) {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	tuning := registerWriterFlags(fs)
	return tuning, fs.Parse(args)
}

// TestRegisterWriterFlagsDefaults asserts that, with no flags and no env vars,
// every knob resolves to the exported clickhouse.Default* constant — the
// single source of truth the README config table also documents.
func TestRegisterWriterFlagsDefaults(t *testing.T) {
	tuning, err := parseWriterFlags(t, nil)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.queueSize != clickhouse.DefaultWriteQueueSize {
		t.Errorf("queueSize = %d, want %d", tuning.queueSize, clickhouse.DefaultWriteQueueSize)
	}
	if tuning.workers != clickhouse.DefaultWriteWorkers {
		t.Errorf("workers = %d, want %d", tuning.workers, clickhouse.DefaultWriteWorkers)
	}
	if tuning.batchMaxRows != clickhouse.DefaultBatchMaxRows {
		t.Errorf("batchMaxRows = %d, want %d", tuning.batchMaxRows, clickhouse.DefaultBatchMaxRows)
	}
	if tuning.batchMaxWait != clickhouse.DefaultBatchMaxWait {
		t.Errorf("batchMaxWait = %s, want %s", tuning.batchMaxWait, clickhouse.DefaultBatchMaxWait)
	}
	if tuning.enqueueTimeout != clickhouse.DefaultEnqueueTimeout {
		t.Errorf("enqueueTimeout = %s, want %s", tuning.enqueueTimeout, clickhouse.DefaultEnqueueTimeout)
	}
	if tuning.drainTimeout != clickhouse.DefaultShutdownDrainTimeout {
		t.Errorf("drainTimeout = %s, want %s", tuning.drainTimeout, clickhouse.DefaultShutdownDrainTimeout)
	}
}

// TestRegisterWriterFlagsParseOverrides covers the flag path for all six knobs:
// an explicit --writer-* flag must win over the default.
func TestRegisterWriterFlagsParseOverrides(t *testing.T) {
	tuning, err := parseWriterFlags(t, []string{
		"--writer-queue-size=1234",
		"--writer-workers=9",
		"--writer-batch-max-rows=250",
		"--writer-batch-max-wait=500ms",
		"--writer-enqueue-timeout=3s",
		"--writer-drain-timeout=45s",
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.queueSize != 1234 {
		t.Errorf("queueSize = %d, want 1234", tuning.queueSize)
	}
	if tuning.workers != 9 {
		t.Errorf("workers = %d, want 9", tuning.workers)
	}
	if tuning.batchMaxRows != 250 {
		t.Errorf("batchMaxRows = %d, want 250", tuning.batchMaxRows)
	}
	if tuning.batchMaxWait != 500*time.Millisecond {
		t.Errorf("batchMaxWait = %s, want 500ms", tuning.batchMaxWait)
	}
	if tuning.enqueueTimeout != 3*time.Second {
		t.Errorf("enqueueTimeout = %s, want 3s", tuning.enqueueTimeout)
	}
	if tuning.drainTimeout != 45*time.Second {
		t.Errorf("drainTimeout = %s, want 45s", tuning.drainTimeout)
	}
}

// TestRegisterWriterFlagsEnvFallback covers the env-twin path: with no flag
// given, each knob picks up its WRITER_* env var. t.Setenv restores the
// environment on cleanup, so these cases don't leak into other tests.
func TestRegisterWriterFlagsEnvFallback(t *testing.T) {
	t.Setenv("WRITER_QUEUE_SIZE", "7000")
	t.Setenv("WRITER_WORKERS", "12")
	t.Setenv("WRITER_BATCH_MAX_ROWS", "2000")
	t.Setenv("WRITER_BATCH_MAX_WAIT", "2s")
	t.Setenv("WRITER_ENQUEUE_TIMEOUT", "750ms")
	t.Setenv("WRITER_DRAIN_TIMEOUT", "30s")

	tuning, err := parseWriterFlags(t, nil)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.queueSize != 7000 {
		t.Errorf("queueSize = %d, want 7000", tuning.queueSize)
	}
	if tuning.workers != 12 {
		t.Errorf("workers = %d, want 12", tuning.workers)
	}
	if tuning.batchMaxRows != 2000 {
		t.Errorf("batchMaxRows = %d, want 2000", tuning.batchMaxRows)
	}
	if tuning.batchMaxWait != 2*time.Second {
		t.Errorf("batchMaxWait = %s, want 2s", tuning.batchMaxWait)
	}
	if tuning.enqueueTimeout != 750*time.Millisecond {
		t.Errorf("enqueueTimeout = %s, want 750ms", tuning.enqueueTimeout)
	}
	if tuning.drainTimeout != 30*time.Second {
		t.Errorf("drainTimeout = %s, want 30s", tuning.drainTimeout)
	}
}

// TestRegisterWriterFlagsFlagBeatsEnv asserts the documented precedence: when
// both an env var and a flag are set, the flag wins.
func TestRegisterWriterFlagsFlagBeatsEnv(t *testing.T) {
	t.Setenv("WRITER_WORKERS", "12")
	t.Setenv("WRITER_ENQUEUE_TIMEOUT", "750ms")

	tuning, err := parseWriterFlags(t, []string{"--writer-workers=3", "--writer-enqueue-timeout=1s"})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.workers != 3 {
		t.Errorf("workers = %d, want 3 (flag must beat env)", tuning.workers)
	}
	if tuning.enqueueTimeout != time.Second {
		t.Errorf("enqueueTimeout = %s, want 1s (flag must beat env)", tuning.enqueueTimeout)
	}
}

// TestRegisterWriterFlagsInvalidEnvFallsBack covers Invariant-5 graceful
// degradation: an unparsable WRITER_* env value must not fail startup — it
// falls back to the shipped default (via getEnvIntOrDefault /
// getEnvDurationOrDefault, which also emit the stderr warning).
func TestRegisterWriterFlagsInvalidEnvFallsBack(t *testing.T) {
	t.Setenv("WRITER_QUEUE_SIZE", "not-a-number")
	t.Setenv("WRITER_WORKERS", "12.5")
	t.Setenv("WRITER_BATCH_MAX_WAIT", "not-a-duration")
	t.Setenv("WRITER_ENQUEUE_TIMEOUT", "10")  // missing unit — invalid duration
	t.Setenv("WRITER_DRAIN_TIMEOUT", "later") // invalid duration

	tuning, err := parseWriterFlags(t, nil)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tuning.queueSize != clickhouse.DefaultWriteQueueSize {
		t.Errorf("queueSize = %d, want default %d on invalid env", tuning.queueSize, clickhouse.DefaultWriteQueueSize)
	}
	if tuning.workers != clickhouse.DefaultWriteWorkers {
		t.Errorf("workers = %d, want default %d on invalid env", tuning.workers, clickhouse.DefaultWriteWorkers)
	}
	if tuning.batchMaxWait != clickhouse.DefaultBatchMaxWait {
		t.Errorf("batchMaxWait = %s, want default %s on invalid env", tuning.batchMaxWait, clickhouse.DefaultBatchMaxWait)
	}
	if tuning.enqueueTimeout != clickhouse.DefaultEnqueueTimeout {
		t.Errorf("enqueueTimeout = %s, want default %s on invalid env",
			tuning.enqueueTimeout, clickhouse.DefaultEnqueueTimeout)
	}
	if tuning.drainTimeout != clickhouse.DefaultShutdownDrainTimeout {
		t.Errorf("drainTimeout = %s, want default %s on invalid env",
			tuning.drainTimeout, clickhouse.DefaultShutdownDrainTimeout)
	}
}

// parseOperatorFlags registers the full operator flag set on a throwaway
// FlagSet, so the operator-level settings are asserted through the same
// registration main() uses.
func parseOperatorFlags(t *testing.T, args []string) (*operatorConfig, *managerFlags, error) {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg, mf := registerFlags(fs)
	return cfg, mf, fs.Parse(args)
}

// TestRegisterFlagsOperatorSettings covers the surviving operator-level
// settings after the CH_* connection flags were retired (D5): the identity and
// tuning that belong to *this operator instance* rather than to any sink.
//
// --operator-namespace has no default on purpose — main() refuses to start
// without it, because guessing the namespace the operator reads Secrets in
// would silently move a security boundary (Task 1.9).
func TestRegisterFlagsOperatorSettings(t *testing.T) {
	tests := []struct {
		name            string
		env             map[string]string
		args            []string
		wantClusterID   string
		wantNamespace   string
		wantWorkers     int
		wantAutoCreate  bool
		wantLeaderElect bool
	}{
		{
			name:          "defaults",
			wantClusterID: "local-kind-cluster",
			wantNamespace: "",
			wantWorkers:   pipeline.DefaultWorkers,
		},
		{
			name: "env fallback",
			env: map[string]string{
				"CLUSTER_ID":            "prod-eu-1",
				"POD_NAMESPACE":         "kuberecord-system",
				"PIPELINE_WORKERS":      "24",
				"CH_AUTO_CREATE_SCHEMA": "true",
			},
			wantClusterID:  "prod-eu-1",
			wantNamespace:  "kuberecord-system",
			wantWorkers:    24,
			wantAutoCreate: true,
		},
		{
			name: "flag beats env",
			env: map[string]string{
				"CLUSTER_ID":       "from-env",
				"POD_NAMESPACE":    "from-env-ns",
				"PIPELINE_WORKERS": "24",
			},
			args: []string{
				"--cluster-id=from-flag", "--operator-namespace=from-flag-ns",
				"--pipeline-workers=3", "--leader-elect",
			},
			wantClusterID:   "from-flag",
			wantNamespace:   "from-flag-ns",
			wantWorkers:     3,
			wantLeaderElect: true,
		},
		{
			name:          "invalid worker count degrades to the default",
			env:           map[string]string{"PIPELINE_WORKERS": "lots"},
			wantClusterID: "local-kind-cluster",
			wantWorkers:   pipeline.DefaultWorkers,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, mf, err := parseOperatorFlags(t, tc.args)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if cfg.clusterID != tc.wantClusterID {
				t.Errorf("clusterID = %q, want %q", cfg.clusterID, tc.wantClusterID)
			}
			if cfg.operatorNamespace != tc.wantNamespace {
				t.Errorf("operatorNamespace = %q, want %q", cfg.operatorNamespace, tc.wantNamespace)
			}
			if cfg.pipelineWorkers != tc.wantWorkers {
				t.Errorf("pipelineWorkers = %d, want %d", cfg.pipelineWorkers, tc.wantWorkers)
			}
			if cfg.autoCreateSchema != tc.wantAutoCreate {
				t.Errorf("autoCreateSchema = %t, want %t", cfg.autoCreateSchema, tc.wantAutoCreate)
			}
			if mf.enableLeaderElection != tc.wantLeaderElect {
				t.Errorf("enableLeaderElection = %t, want %t", mf.enableLeaderElection, tc.wantLeaderElect)
			}
		})
	}
}

// TestRegisterFlagsRetiredClickHouseFlags is the executable half of D5: the
// connection settings moved to the ClickHouseSink CR + Secret, so the binary
// must not accept them any more. A flag that lingered would silently do nothing
// while looking like configuration.
func TestRegisterFlagsRetiredClickHouseFlags(t *testing.T) {
	for _, name := range []string{
		"ch-addr", "ch-database", "ch-username", "ch-dial-timeout", "ch-read-timeout",
	} {
		t.Run(name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			registerFlags(fs)
			if f := fs.Lookup(name); f != nil {
				t.Errorf("--%s is still registered; ClickHouse connection settings live on the CR (D5)", name)
			}
		})
	}
}

// duration is a shorthand for the optional metav1.Duration fields the sink
// spec uses.
func duration(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

// int32Ptr is a shorthand for the optional int32 fields the writer spec uses.
func int32Ptr(n int32) *int32 { return &n }

// TestBuildSinkConfig covers the one mapping cmd/main.go owns: a ClickHouseSink
// spec plus its resolved password onto the backend configuration the sink
// runtime builds an instance from.
//
// The two directions that matter are the fallback contract (a field the CR omits
// takes the operator's --writer-* value, not the package default) and its
// converse (a field the CR states always wins). Both are asserted per field,
// because a mapping this wide is exactly where a copy-paste error hides.
func TestBuildSinkConfig(t *testing.T) {
	defaults := writerTuning{
		queueSize:      111,
		workers:        2,
		batchMaxRows:   333,
		batchMaxWait:   3 * time.Second,
		enqueueTimeout: 4 * time.Second,
		drainTimeout:   5 * time.Second,
	}
	connection := v1alpha1.ConnectionSpec{
		Addr:        "clickhouse.example.svc:9000",
		Database:    "kuberecord",
		Username:    "writer",
		DialTimeout: duration(7 * time.Second),
		ReadTimeout: duration(11 * time.Second),
	}

	tests := []struct {
		name         string
		spec         v1alpha1.ClickHouseSinkSpec
		autoCreate   bool
		wantConfig   clickhouse.Config
		wantPassword string
	}{
		{
			name: "an omitted writer block falls back to the operator defaults",
			spec: v1alpha1.ClickHouseSinkSpec{Connection: connection},
			wantConfig: clickhouse.Config{
				Addr:                 connection.Addr,
				Database:             connection.Database,
				Username:             connection.Username,
				Password:             "s3cret",
				DialTimeout:          7 * time.Second,
				ReadTimeout:          11 * time.Second,
				WriteQueueSize:       defaults.queueSize,
				WriteWorkers:         defaults.workers,
				BatchMaxRows:         defaults.batchMaxRows,
				BatchMaxWait:         defaults.batchMaxWait,
				EnqueueTimeout:       defaults.enqueueTimeout,
				ShutdownDrainTimeout: defaults.drainTimeout,
				// checkpointEvery has no --writer-* twin (the CRD defaults it), so
				// an omitted field falls back to the shipped cadence itself.
				CheckpointEvery: clickhouse.DefaultCheckpointEvery,
			},
		},
		{
			name: "a writer block on the CR wins over the operator defaults",
			spec: v1alpha1.ClickHouseSinkSpec{
				Connection: connection,
				Writer: v1alpha1.WriterSpec{
					QueueSize:       int32Ptr(9000),
					Workers:         int32Ptr(16),
					BatchMaxRows:    int32Ptr(2500),
					BatchMaxWait:    duration(250 * time.Millisecond),
					EnqueueTimeout:  duration(time.Second),
					DrainTimeout:    duration(30 * time.Second),
					CheckpointEvery: int32Ptr(7),
				},
			},
			autoCreate: true,
			wantConfig: clickhouse.Config{
				Addr:                 connection.Addr,
				Database:             connection.Database,
				Username:             connection.Username,
				Password:             "s3cret",
				DialTimeout:          7 * time.Second,
				ReadTimeout:          11 * time.Second,
				AutoCreateSchema:     true,
				WriteQueueSize:       9000,
				WriteWorkers:         16,
				BatchMaxRows:         2500,
				BatchMaxWait:         250 * time.Millisecond,
				EnqueueTimeout:       time.Second,
				ShutdownDrainTimeout: 30 * time.Second,
				CheckpointEvery:      7,
			},
		},
		{
			name: "omitted connection timeouts fall back to the CRD's own defaults",
			spec: v1alpha1.ClickHouseSinkSpec{
				Connection: v1alpha1.ConnectionSpec{
					Addr:     connection.Addr,
					Database: connection.Database,
					Username: connection.Username,
				},
			},
			wantConfig: clickhouse.Config{
				Addr:                 connection.Addr,
				Database:             connection.Database,
				Username:             connection.Username,
				Password:             "s3cret",
				DialTimeout:          defaultSinkDialTimeout,
				ReadTimeout:          defaultSinkReadTimeout,
				WriteQueueSize:       defaults.queueSize,
				WriteWorkers:         defaults.workers,
				BatchMaxRows:         defaults.batchMaxRows,
				BatchMaxWait:         defaults.batchMaxWait,
				EnqueueTimeout:       defaults.enqueueTimeout,
				ShutdownDrainTimeout: defaults.drainTimeout,
				CheckpointEvery:      clickhouse.DefaultCheckpointEvery,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			build := newSinkConfigBuilder(defaults, tc.autoCreate)
			got, err := build("default", tc.spec, "s3cret")
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			chConfig, ok := got.(clickhouse.Config)
			if !ok {
				t.Fatalf("build returned %T, want clickhouse.Config", got)
			}
			if chConfig != tc.wantConfig {
				t.Errorf("config =\n\t%+v\nwant\n\t%+v", chConfig, tc.wantConfig)
			}
		})
	}
}

// TestBuildSinkConfigFingerprintsThePassword guards the credential-rotation
// path end to end from this side: the builder must carry the password into the
// configuration, because the fingerprint the sink runtime recycles on is
// computed from it. A builder that dropped the password would leave a rotated
// Secret producing an identical fingerprint — and a sink still authenticating
// with the old credential until the process restarted.
func TestBuildSinkConfigFingerprintsThePassword(t *testing.T) {
	build := newSinkConfigBuilder(writerTuning{}, false)
	spec := v1alpha1.ClickHouseSinkSpec{
		Connection: v1alpha1.ConnectionSpec{Addr: "ch:9000", Database: "kuberecord", Username: "writer"},
	}

	before, err := build("default", spec, "old-password")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	after, err := build("default", spec, "new-password")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if before.Fingerprint() == after.Fingerprint() {
		t.Error("fingerprint is unchanged after a password rotation; the sink would never be recycled")
	}
}

// TestSinkFactoryRejectsAForeignConfig covers the D6 seam: the factory is the
// only place that knows ClickHouse exists, and a configuration built for some
// future backend must be refused with a legible error rather than panic in a
// type assertion on a lifecycle goroutine.
func TestSinkFactoryRejectsAForeignConfig(t *testing.T) {
	factory := newSinkFactory(pipeline.NewPipelineMetrics(prometheus.NewRegistry()))
	if _, err := factory(sink.ID{Kind: sink.DefaultSinkKind, Name: "default"}, foreignConfig{}); err == nil {
		t.Fatal("factory accepted a non-ClickHouse configuration, want an error")
	}
}

// foreignConfig stands in for a future backend's InstanceConfig (D6).
type foreignConfig struct{}

func (foreignConfig) Fingerprint() string { return "foreign" }

// TestManagerCacheOptionsConfineSecretsToTheOperatorNamespace guards the one
// wiring detail that decides whether the operator can run under its own RBAC.
//
// The Secret grant is a namespaced Role (Task 1.9, D7), so the manager's Secret
// informer has to issue a namespaced list. Left at the default it lists
// cluster-wide, the API server refuses it, the cache never syncs and every
// ClickHouseSink hangs with an empty status — a failure envtest cannot reproduce,
// because its client is effectively an administrator. Hence a unit test on the
// options themselves.
func TestManagerCacheOptionsConfineSecretsToTheOperatorNamespace(t *testing.T) {
	const namespace = "kuberecord-system"

	opts := managerCacheOptions(namespace)

	var found bool
	for object, byObject := range opts.ByObject {
		if _, ok := object.(*corev1.Secret); !ok {
			continue
		}
		found = true
		if _, ok := byObject.Namespaces[namespace]; !ok {
			t.Errorf("Secret cache is not scoped to %q; namespaces = %v", namespace, byObject.Namespaces)
		}
		if len(byObject.Namespaces) != 1 {
			t.Errorf("Secret cache covers %d namespaces, want exactly the operator's one",
				len(byObject.Namespaces))
		}
	}
	if !found {
		t.Fatal("no per-object cache configuration for Secrets; the informer would list cluster-wide")
	}
}
