# Operating kubestream

kubestream watches your cluster, so something has to watch kubestream. This page
is the short version of that: which signals matter, where they come from, and
what to do when one of them goes the wrong way.

Two artifacts ship with the operator:

- [`deploy/grafana/operator-health.json`](../deploy/grafana/operator-health.json) —
  a Grafana dashboard covering the whole write path and the state of your rules.
- [`deploy/prometheus/alerts.yaml`](../deploy/prometheus/alerts.yaml) — four
  sample alerts, each with the reasoning for its threshold written above it.

Both are built entirely on the metrics the operator already exports. Neither
requires anything but a Prometheus that scrapes the operator.

This page is about watching **the operator**. For reading the cluster history it
records — object timelines, GitOps drift, flapping objects, namespace activity —
see [`docs/DASHBOARDS.md`](DASHBOARDS.md) and the SQL behind it in
[`docs/QUERIES.md`](QUERIES.md). Those dashboards read ClickHouse, not Prometheus,
and need the ClickHouse data source plugin rather than this page's setup.

## Getting the metrics

The metrics endpoint is off unless you ask for it: `--metrics-bind-address`
defaults to `0`. Set it to `:8443` for HTTPS (authenticated by `TokenReview` and
authorized by `SubjectAccessReview`) or to `:8080` with `--metrics-secure=false`
for plain HTTP on a local cluster. The Helm chart turns it on by default —
`metrics.enabled: true` passes the argument and creates the `Service` — so with
the chart you only have to point a `ServiceMonitor` or a scrape config at it.

Everything kubestream exports is prefixed `kuberecord_`, alongside the standard
`controller_runtime_*`, `workqueue_*` and `go_*` families the manager publishes on
its own — the two `workqueue_*` families are worth a look too, since the pipeline's
own queue is a client-go workqueue.

Every metric a single sink instance reports carries a `sink=<name>` label naming
the `ClickHouseSink` it belongs to: one operator can run several sinks at once,
and without the label two writers would overwrite each other's series. Metrics
the shared pipeline owns (`dedup_skips_total`, `pipeline_dropped_total`) carry no
`sink` label, because they describe the workqueue rather than any one backend. A
sink's series are deleted when the sink is deleted, so an absent backend does not
linger as a live-but-idle one.

| Metric | Type | Labels | What it tells you |
|---|---|---|---|
| `kuberecord_write_queue_depth` | gauge | `sink` | Jobs buffered in a sink's hand-off queue right now. |
| `kuberecord_write_queue_capacity` | gauge | `sink` | That queue's ceiling. Moves only when the sink is reconfigured. |
| `kuberecord_writes_total` | counter | `sink`, `outcome` | Settled write jobs. Counted once per job, at settle, after retries. |
| `kuberecord_write_latency_seconds` | histogram | `sink` | First attempt to final settle, retries included. |
| `kuberecord_write_retry_attempts_total` | counter | `sink` | Attempts beyond the first. |
| `kuberecord_write_batch_rows` | histogram | `sink` | Rows in each flushed insert batch. |
| `kuberecord_enqueue_block_seconds` | histogram | `sink` | How long the hot path waited for queue room. |
| `kuberecord_enqueue_timeouts_total` | counter | `sink` | Enqueues that gave up because the queue stayed full. |
| `kuberecord_dedup_skips_total` | counter | — | Work items short-circuited by an unchanged hash. |
| `kuberecord_hashcache_entries` | gauge | `sink` | Live dedup-baseline entries; the in-memory footprint. |
| `kuberecord_safe_mode` | gauge | `sink`, `group`, `kind`, `namespace` | 1 while a scope is still warming its baseline from sink history. |
| `kuberecord_pipeline_dropped_total` | counter | `reason` | Items deliberately discarded: `scope_stopped` (the scope was deactivated first) or `ephemeral_delete` (a Kubernetes Event's TTL expired — expected to tick continuously wherever Events are streamed). |
| `kuberecord_rules` | gauge | `condition`, `status` | How many rules hold each condition at each status. |

`kuberecord_rules` is the control plane's only metric, and it is deliberately
identity-free: it counts rules, it does not name them. Both `StreamRule` and
`ClusterStreamRule` count into the same series. Once it tells you a rule is
degraded, `kubectl` tells you which:

```console
$ kubectl get streamrules,clusterstreamrules -A
$ kubectl describe clusterstreamrule <name>   # the conditions carry the reason
```

## Installing the dashboard

Import the JSON in Grafana (**Dashboards → New → Import → Upload JSON file**) and
pick your Prometheus when prompted. The dashboard has a `Data source` variable, so
nothing in it is pinned to one Grafana install, and a `Sink` variable for clusters
streaming to more than one `ClickHouseSink`. Its UID is `kubestream-operator-health`,
so re-importing an updated copy replaces it in place rather than creating a second one.

The panels, and what each is for:

| Panel | Read it when |
|---|---|
| Write queue depth vs capacity | Always. The gap between the two lines is your headroom. |
| Write outcomes | Any `failed` series at all is rows that never reached ClickHouse. |
| Write latency p99 | The queue is filling but nothing is failing — a slow backend, not a broken one. |
| Retry rate | Latency looks fine but throughput does not: the writer is re-attempting. |
| Batch size distribution | Tuning. Mass near 1 means batches are flushing on the timer, not filling. |
| Enqueue backpressure | The queue is full and you want to know whether work is being shed yet. |
| Degraded rules | Always. Anything above zero means somebody's rule is not streaming. |
| SafeMode scopes | After a restart or a new rule; should fall to zero within seconds. |
| SafeMode scopes — which ones | The count above refuses to fall. |

## Installing the alerts

```console
$ kubectl apply -f deploy/prometheus/alerts.yaml
```

The file is a prometheus-operator `PrometheusRule` labelled
`release: kube-prometheus-stack`, which is what a stock kube-prometheus-stack
selects on — change the label to match your own `ruleSelector`. If your Prometheus
is not operator-managed, lift `.spec` into a file and list it under `rule_files:`;
everything below the `spec` key is an ordinary Prometheus rule file.

| Alert | Fires on | After | Severity |
|---|---|---|---|
| `KubestreamWriteQueueSaturated` | queue over 80% of capacity | 5m | warning |
| `KubestreamWriteFailures` | any nonzero failed-write rate | 10m | critical |
| `KubestreamRuleNotReady` | any rule with `Ready=False` | 15m | warning |
| `KubestreamEnqueueTimeouts` | any nonzero enqueue-timeout rate | 5m | critical |

The thresholds are argued in comments in the file itself; the short version is
that the two "any nonzero rate" alerts have no meaningful threshold to tune (a
lost write is lost) so the tuning lives entirely in the `for` duration, while the
two levelled signals — 80% queue occupancy, 15 minutes of a not-Ready rule — are
each set where the operator's own self-healing has demonstrably stopped working.

### Queue saturation

The hand-off queue is a shock absorber and filling it briefly is normal: an
informer re-list or a mass rollout hands the pipeline thousands of objects at
once. Sustained occupancy above 80% is different — ClickHouse is draining slower
than your cluster is changing.

Check write latency and the retry rate first. If the backend is healthy and simply
outpaced, raise `spec.writer.workers` (more concurrent inserts) or
`spec.writer.queueSize` (more buffer) on the `ClickHouseSink`; see
[`docs/PERFORMANCE.md`](PERFORMANCE.md) for the measured envelopes. If latency is
climbing, the fix is on the ClickHouse side.

### Failed writes

A write settles as `failed` only after exhausting its retry budget, so this is not
"a write was slow" — it is rows that are not in ClickHouse and gaps in the audit
trail. Read the `ClickHouseSink`'s conditions (`kubectl describe clickhousesink
<name>`): an unreachable backend, a rejected credential and a schema mismatch each
report a distinct reason. The operator keeps its watches running throughout, and
recovers without re-emitting the world, so there is nothing to restart.

### Rules not ready

A rule can legitimately be `Ready=False` for a few minutes during ordinary
sequencing — applied before its sink exists, naming a CRD that is not installed
yet, waiting on an RBAC preset. All of those self-heal on the reconciler's
two-minute resync, which is why the alert waits 15 minutes.

Past that, read the conditions. The usual causes are a missing watcher preset
(`RBACGranted=False` — see [`docs/RBAC.md`](RBAC.md)), a kind that never resolves
(`ResourceResolved=False`), or a sink policy denial (`PolicyAllowed=False`). The
operator can never grant itself the missing rights, so an administrator has to
apply the preset; the condition message names the exact resource and verbs.

### Enqueue timeouts

An enqueue timeout means the queue stayed full for the whole enqueue timeout and
the pipeline gave up handing that item over. Nothing is lost — the item goes back
on the workqueue and the informer cache still holds the object's latest state —
but the pipeline has stopped keeping up, and every requeue adds to how far behind
ClickHouse is. Treat it as the escalation of queue saturation above; seeing both
at once means the sink is undersized rather than briefly slow.

## Validating changes to these files

```console
$ make verify-observability
```

Both artifacts are validated against JSON Schemas
([`deploy/grafana/dashboard.schema.json`](../deploy/grafana/dashboard.schema.json),
[`deploy/prometheus/prometheusrule.schema.json`](../deploy/prometheus/prometheusrule.schema.json)),
the alert rules are additionally parsed by `promtool check rules`, and every
`kuberecord_*` metric either file queries is checked against the set the
operator's collectors declare — so renaming a metric fails the build instead of
quietly emptying a panel. The schema half runs under plain `make test`; the
target above adds promtool, which it downloads into `bin/`. CI runs both.

> **Deviation worth knowing.** Grafana publishes no stable, self-contained JSON
> Schema for a dashboard — the model is a Go/TypeScript type that changes shape
> between minor releases. `dashboard.schema.json` is therefore a curated subset
> written for this repository: it pins the fields a dashboard must get right to
> import cleanly and stay reviewable, and allows unknown properties everywhere so
> a dashboard re-exported from a newer Grafana still validates. It is a
> lint-grade check, not a claim of full conformance.
