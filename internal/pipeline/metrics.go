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

package pipeline

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// metricsNamespace prefixes every metric name with "kuberecord_", giving the
// operator a single, greppable namespace on the /metrics endpoint that
// controller-runtime already serves via --metrics-bind-address.
const metricsNamespace = "kuberecord"

// sinkLabel is the label every collector a single sink instance reports through
// carries, so a cluster streaming to two sinks can tell their queues, latencies
// and failures apart.
//
// It is not cosmetic. Before Task 1.8 there was exactly one writer in the
// process, so an unlabelled write_queue_depth gauge described it unambiguously;
// with one instance per ClickHouseSink CR, two writers publishing to one gauge
// would simply overwrite each other and the resulting series would describe
// whichever happened to report last. Where the label stops is equally
// deliberate: metrics the *pipeline* owns (dedup skips, dropped items) stay
// unlabelled, because they are properties of the shared workqueue rather than of
// any one backend.
//
// Its *value* is sink.ID.String() — "<Kind>/<Name>", e.g.
// "ClickHouseSink/default" — not the bare CR name (Task 4.1). The kind belongs in
// the label because a name is only unique within a kind: a ClickHouseSink and an
// S3Sink may both be called "default", and labelled by name alone their series
// would merge into one that describes neither backend.
const sinkLabel = "sink"

// PipelineMetrics is the full set of Prometheus collectors describing the
// write pipeline's health: queue saturation, write outcomes and latency,
// retry storms, dedup short-circuits, cache size, per-scope Snapshot (safe)
// mode, and dropped work items. Before this, all of those were only visible in
// logs; every later performance task (0.6, 0.8, 2.3) needs them to prove it
// actually helped.
//
// It is exported because the ClickHouse writer (internal/sink/clickhouse)
// records the write-path metrics through the narrow clickhouse.Metrics
// interface, which the per-sink view returned by ForSink satisfies. The
// collector fields stay unexported: callers mutate them only through that view
// or through this package's own code.
//
// Collectors are grouped in a struct (rather than package-level vars) so tests
// can build an isolated instance against a fresh registry — Prometheus panics
// on duplicate registration, so a package-level singleton alone would make
// repeated test setups fatal. Production code uses exactly one instance,
// registered once on controller-runtime's global registry (see
// PipelineMetricsInstance).
type PipelineMetrics struct {
	// writeQueueDepth / writeQueueCapacity together show how close a sink's
	// bounded hand-off queue is to saturation — the earliest warning that the
	// backend can't keep up with the observed change rate.
	writeQueueDepth    *prometheus.GaugeVec
	writeQueueCapacity *prometheus.GaugeVec

	// writesTotal counts settled write outcomes, labelled success|failed, so a
	// rising failed rate is distinguishable from a healthy throughput dip.
	writesTotal *prometheus.CounterVec
	// writeLatency measures a single job's time from first attempt to final
	// settle (including retries), the direct signal of sink responsiveness.
	writeLatency *prometheus.HistogramVec
	// writeRetryAttempts counts every attempt beyond the first, surfacing
	// retry storms that writesTotal alone hides (a write can succeed after
	// many retries and still count only once as a success).
	writeRetryAttempts *prometheus.CounterVec
	// writeBatchRows records the number of rows in each flushed ClickHouse
	// batch, observed once per flush. It is the direct signal of how well the
	// batcher is coalescing: a distribution clustered near batchMaxRows means
	// full, efficient batches, while a mass near 1 means trickle traffic is
	// flushing on the batchMaxWait timer instead of filling batches.
	writeBatchRows *prometheus.HistogramVec

	// enqueueBlock measures how long Enqueue blocks waiting for queue room —
	// the hot-path backpressure the pipeline's workers actually feel.
	// enqueueTimeouts counts the cases where that wait gave up, i.e. the queue
	// stayed full.
	enqueueBlock    *prometheus.HistogramVec
	enqueueTimeouts *prometheus.CounterVec

	// dedupSkips counts Process calls that short-circuited because the object's
	// hash was unchanged — the proportion of work the hashCache saves.
	dedupSkips prometheus.Counter

	// hashcacheEntries reports the live entry count per sink, the in-memory
	// baseline footprint that Task 0.7 works to shrink. It is labelled by sink
	// rather than by kind because there is exactly one hashCache per sink,
	// spanning every kind that sink receives (the version-agnostic identity key
	// makes a single map correct and a per-kind breakdown unavailable without a
	// second index the hot path would have to maintain).
	hashcacheEntries *prometheus.GaugeVec
	// safeMode is 1 while a (sink, scope) pair's cache is still warming from
	// sink history (cache-misses tagged Snapshot, not Added) and 0 once warm.
	// Warm-up is per-scope now: a rule created hours after boot warms only its
	// own scope, so readiness cannot be a single per-kind flag.
	//
	// On a Writer-only sink (D12) it stays at 1 for every scope, forever, because
	// no scope is ever marked warm — and that is the intended reading, not a
	// missing transition. This gauge is the metrics-side observation of the same
	// fact the sink's HistoryUnavailable=True condition states, which is why there
	// is deliberately no second series saying it: a parallel "writer-only" metric
	// would be one more thing to keep in agreement with this one, and a dashboard
	// that watched only the new one would stop noticing an *ordinary* sink stuck
	// warming. An alert distinguishes the two cases by duration — a scope that has
	// been in Snapshot mode for hours is either a broken sink or an archive tier,
	// and the sink's own condition says which.
	safeMode *prometheus.GaugeVec

	// dropped counts work items the pipeline deliberately discarded, by reason.
	// A drop is never an error, and never a deletion — both reasons are cases
	// where recording anything would be an outright lie:
	//
	//   - scope_stopped: the item's watch target was deactivated while the item
	//     sat in the queue, so there is nothing left to observe it through and a
	//     Deleted row would say "it was deleted" (see the scope-epoch design). A
	//     persistently nonzero rate means rules are churning faster than the
	//     pipeline drains.
	//   - ephemeral_delete: a Kubernetes Event's TTL expired (see
	//     DropReasonEphemeralDelete). Unlike the first, a steady rate here is the
	//     healthy state wherever Events are streamed.
	dropped *prometheus.CounterVec
}

// DropReasonScopeStopped labels a work item discarded because its watch scope
// was no longer active by the time a worker picked the item up.
const DropReasonScopeStopped = "scope_stopped"

// DropReasonEphemeralDelete labels a work item for a Kubernetes Event that has
// left the watch cache — its ~1h TTL expired — which is deliberately recorded as
// nothing at all rather than as a Deleted row (see ephemeralKind).
//
// It is a counter and not merely a log line because this is the one drop reason
// with a *healthy* nonzero rate: on a cluster streaming Events it ticks
// continuously, and its shape is the cheapest available proxy for the Event
// churn the operator is absorbing. A rate of zero on a scope that is streaming
// Events, on the other hand, means expiries are not being observed at all.
const DropReasonEphemeralDelete = "ephemeral_delete"

// NewPipelineMetrics constructs every collector and registers it on reg.
// Registration uses MustRegister, so passing a registry that already holds
// these names panics — that is intentional: production passes the global
// registry exactly once (guarded by sync.Once in PipelineMetricsInstance),
// and each test passes its own fresh registry.
func NewPipelineMetrics(reg prometheus.Registerer) *PipelineMetrics {
	m := &PipelineMetrics{
		writeQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "write_queue_depth",
			Help:      "Current number of jobs buffered in a sink's hand-off queue.",
		}, []string{sinkLabel}),
		writeQueueCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "write_queue_capacity",
			Help:      "Maximum number of jobs a sink's hand-off queue can buffer.",
		}, []string{sinkLabel}),
		writesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "writes_total",
			Help:      "Count of settled sink write jobs by sink and outcome.",
		}, []string{sinkLabel, "outcome"}),
		writeLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "write_latency_seconds",
			Help:      "Time from a write job's first attempt to its final settle, including retries.",
			Buckets:   prometheus.DefBuckets,
		}, []string{sinkLabel}),
		writeRetryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "write_retry_attempts_total",
			Help:      "Count of write attempts beyond the first (i.e. retries) across all jobs, by sink.",
		}, []string{sinkLabel}),
		writeBatchRows: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "write_batch_rows",
			Help:      "Number of rows in each flushed insert batch, by sink.",
			// Exponential buckets 1..2048 span a single trickle row through a
			// full batch at any realistic batchMaxRows setting.
			Buckets: prometheus.ExponentialBuckets(1, 2, 12),
		}, []string{sinkLabel}),
		enqueueBlock: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Name:      "enqueue_block_seconds",
			Help:      "Time Enqueue spent blocked waiting for room in a sink's write queue.",
			Buckets:   prometheus.DefBuckets,
		}, []string{sinkLabel}),
		enqueueTimeouts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "enqueue_timeouts_total",
			Help:      "Count of Enqueue calls that gave up because a sink's queue stayed full past the timeout.",
		}, []string{sinkLabel}),
		dedupSkips: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Name:      "dedup_skips_total",
			Help:      "Count of pipeline work items short-circuited because the object's hash was unchanged.",
		}),
		hashcacheEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "hashcache_entries",
			Help:      "Current number of live hashCache entries, by sink.",
		}, []string{"sink"}),
		safeMode: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace,
			Name:      "safe_mode",
			Help:      "1 while a (sink, scope) pair's cache is still warming (Snapshot mode), 0 once warm; pinned at 1 for every scope on a Writer-only sink.",
		}, []string{"sink", "group", "kind", "namespace"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: "pipeline",
			Name:      "dropped_total",
			Help:      "Count of pipeline work items deliberately discarded, by reason.",
		}, []string{"reason"}),
	}

	// The write-outcome series are materialized per sink instead, by ForSink: the
	// outcome half of the label set is a fixed enum, but the sink half is not
	// known until a sink exists, and seeding a placeholder here would publish a
	// series for a sink nobody configured.
	//
	// The drop reasons still seed here: that set is a fixed enum with no sink
	// dimension, so the series exists (at 0) before the first drop ever happens.
	m.dropped.WithLabelValues(DropReasonScopeStopped)
	m.dropped.WithLabelValues(DropReasonEphemeralDelete)

	reg.MustRegister(
		m.writeQueueDepth,
		m.writeQueueCapacity,
		m.writesTotal,
		m.writeLatency,
		m.writeRetryAttempts,
		m.writeBatchRows,
		m.enqueueBlock,
		m.enqueueTimeouts,
		m.dedupSkips,
		m.hashcacheEntries,
		m.safeMode,
		m.dropped,
	)
	return m
}

var (
	pipelineMetricsOnce      sync.Once
	pipelineMetricsSingleton *PipelineMetrics
)

// PipelineMetricsInstance returns the process-wide PipelineMetrics, registered
// exactly once on controller-runtime's global registry so the existing
// --metrics-bind-address server exposes them. The sync.Once guard makes
// repeated calls (e.g. the ClickHouse writer plus the pipeline itself both
// fetching it) safe and non-duplicating.
func PipelineMetricsInstance() *PipelineMetrics {
	pipelineMetricsOnce.Do(func() {
		pipelineMetricsSingleton = NewPipelineMetrics(ctrlmetrics.Registry)
	})
	return pipelineMetricsSingleton
}

// SinkMetrics is one sink's view of the write-path collectors: every series it
// touches is pinned to sink=<name>.
//
// It implements the clickhouse.Metrics interface — the write-path slice of
// PipelineMetrics, exposed as behavior rather than raw fields so the clickhouse
// package depends on a narrow contract and never imports this one. Each sink
// instance is built with its own view (see ForSink), which is what makes
// "which backend is falling behind?" answerable from metrics alone once more than
// one sink is live.
//
// The child collectors are resolved once, here, rather than per observation: the
// write path calls SetWriteQueueDepth on every enqueue and every flush, and a
// WithLabelValues lookup per call would put a map hash and a mutex on that path
// for a label value that never changes.
type SinkMetrics struct {
	queueDepth    prometheus.Gauge
	queueCapacity prometheus.Gauge
	writeSuccess  prometheus.Counter
	writeFailed   prometheus.Counter
	latency       prometheus.Observer
	retries       prometheus.Counter
	batchRows     prometheus.Observer
	enqueueBlock  prometheus.Observer
	enqueueGaveUp prometheus.Counter
}

// ForSink returns the write-path metrics view for the sink id, seeding both of
// its outcome counters at 0 so dashboards and rate() queries over them are
// well-defined from the moment the sink exists rather than from its first settled
// write.
//
// It is called once per sink instance, by whoever builds that instance (Task
// 1.8's sink factory), because internal/sink cannot import this package: the
// factory receives the sink's identity and is the only place where an identity
// and a backend meet.
func (m *PipelineMetrics) ForSink(id sink.ID) *SinkMetrics {
	label := id.String()
	return &SinkMetrics{
		queueDepth:    m.writeQueueDepth.WithLabelValues(label),
		queueCapacity: m.writeQueueCapacity.WithLabelValues(label),
		writeSuccess:  m.writesTotal.WithLabelValues(label, "success"),
		writeFailed:   m.writesTotal.WithLabelValues(label, "failed"),
		latency:       m.writeLatency.WithLabelValues(label),
		retries:       m.writeRetryAttempts.WithLabelValues(label),
		batchRows:     m.writeBatchRows.WithLabelValues(label),
		enqueueBlock:  m.enqueueBlock.WithLabelValues(label),
		enqueueGaveUp: m.enqueueTimeouts.WithLabelValues(label),
	}
}

// deleteSinkSeries drops every per-sink series for id.
//
// It runs when a sink's pipeline state is discarded (see RemoveSink), for the
// same reason the hashcache_entries series is dropped there: a gauge left behind
// keeps reporting a queue depth and capacity for a backend the operator no longer
// writes to, which reads as a live-but-idle sink rather than an absent one.
func (m *PipelineMetrics) deleteSinkSeries(id sink.ID) {
	label := id.String()
	m.writeQueueDepth.DeleteLabelValues(label)
	m.writeQueueCapacity.DeleteLabelValues(label)
	m.writesTotal.DeleteLabelValues(label, "success")
	m.writesTotal.DeleteLabelValues(label, "failed")
	m.writeLatency.DeleteLabelValues(label)
	m.writeRetryAttempts.DeleteLabelValues(label)
	m.writeBatchRows.DeleteLabelValues(label)
	m.enqueueBlock.DeleteLabelValues(label)
	m.enqueueTimeouts.DeleteLabelValues(label)
	// safe_mode carries the scope dimension as well, and a deleted sink may still
	// have had warming scopes when it went away (its rules are parked *after* the
	// sink is gone), so its series are matched on the sink label alone rather than
	// re-derived from a scope list this call does not have.
	m.safeMode.DeletePartialMatch(prometheus.Labels{sinkLabel: label})
}

// SetWriteQueueDepth publishes this sink's current hand-off queue depth.
func (s *SinkMetrics) SetWriteQueueDepth(n float64) { s.queueDepth.Set(n) }

// SetWriteQueueCapacity publishes this sink's fixed hand-off queue capacity.
func (s *SinkMetrics) SetWriteQueueCapacity(n float64) { s.queueCapacity.Set(n) }

// ObserveEnqueueBlock records how long an Enqueue blocked waiting for room.
func (s *SinkMetrics) ObserveEnqueueBlock(seconds float64) { s.enqueueBlock.Observe(seconds) }

// IncEnqueueTimeout counts an Enqueue that gave up because the queue stayed full.
func (s *SinkMetrics) IncEnqueueTimeout() { s.enqueueGaveUp.Inc() }

// ObserveWriteLatency records a job's first-attempt-to-final-settle latency.
func (s *SinkMetrics) ObserveWriteLatency(seconds float64) { s.latency.Observe(seconds) }

// IncWriteRetryAttempt counts one write attempt beyond the first.
func (s *SinkMetrics) IncWriteRetryAttempt() { s.retries.Inc() }

// ObserveWriteBatchRows records the row count of one flushed insert batch.
func (s *SinkMetrics) ObserveWriteBatchRows(rows float64) { s.batchRows.Observe(rows) }

// IncWrite counts one settled write by outcome ("success" | "failed"). An
// unrecognized outcome is counted as a failure rather than dropped: the caller's
// contract is a two-value enum, so a third value is a bug whose writes must not
// silently vanish from the accounting (Invariant 4).
func (s *SinkMetrics) IncWrite(outcome string) {
	if outcome == "success" {
		s.writeSuccess.Inc()
		return
	}
	s.writeFailed.Inc()
}
