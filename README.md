# kubestream

**Git blame for your Kubernetes cluster.** A lightweight operator that streams every Pod, Service, and Deployment state change into ClickHouse — so "what did this look like before it broke?" has an answer.

## Overview & Motivation

Kubernetes' own audit trail is short-lived and symptom-focused. `kubectl get events` shows you the last hour or so of what happened, native audit logs capture *API calls* rather than *resulting object state*, and once a Pod is evicted or a Deployment is rolled back, the exact spec/status that existed five minutes before the incident is gone. Incident post-mortems end up reconstructing history from a mix of memory, dashboards, and luck.

`kubestream` is a Kubernetes operator (built on `controller-runtime`/`client-go`) that watches a configurable set of resource types and writes every observed state transition — `Added`, `Modified`, `Deleted` — into a ClickHouse table as an immutable, append-only row. Because it hashes and diffs each object's normalized JSON, it only writes when something actually changed, giving you a compact, queryable, retrospective timeline of cluster state instead of a live-only snapshot.

The watched set is never a compiled-in list: it is declarative runtime configuration, so extending coverage to other built-in types or CRDs (plus a matching RBAC grant) is a configuration change, not a code change.

### Status

The operator is mid-migration to a two-tier, CRD-driven architecture (`ClickHouseSink`, `StreamRule`, `ClusterStreamRule`). The data plane has already been rebuilt as a workqueue pipeline (`internal/pipeline`) fed by a dynamic watch manager (`internal/watch`) that starts and stops informers at runtime from the desired-state registry, replacing the per-GVK controller-runtime reconcilers and the environment-variable GVK list that configured them (both deleted — there is no compatibility shim). The scope-epoch recorder, the multi-sink runtime, and now the control-plane reconcilers that translate the CRDs into registry entries and sink configurations (`internal/controller`) all exist.

What is left in Phase 1 is the wiring: `cmd/main.go` still opens a single ClickHouse connection from `CH_*` environment variables and constructs none of the reconcilers, so applying a rule still has no runtime effect and the `CH_*` configuration below is still what the binary reads. Assembling the registry, the watch manager, the sink runtime and the reconcilers in `main` — and deleting the `CH_*` flags — is the remaining task, along with the aggregated `kubestream-watcher` ClusterRole that grants the watch rights rules ask for.

The base `ClusterRole` (`config/rbac/role.yaml`) is generated from the control-plane reconcilers' `+kubebuilder:rbac` markers and now grants what they actually need: read access to the three kubestream CRDs and their status subresources, `namespaces` get/list/watch (for `ClusterStreamRule`'s namespace selector), `selfsubjectaccessreviews` create (for the per-target RBAC checks), and event creation. Standing read access to Pods, Services and Deployments is gone for good — the deleted per-GVK reconcilers carried it, and rules now request watch rights explicitly. Secret reads are a **namespaced** `Role` rather than part of the ClusterRole, which is what makes `credentialsSecretRef.namespace`'s default a real boundary; its `RoleBinding` and the aggregated `kubestream-watcher` role arrive with the RBAC task.

## Use Cases

- **Incident post-mortems** — "what was this Deployment's spec/status at 03:14 UTC, right before the outage started?" Query ClickHouse instead of hoping someone took a screenshot.
- **Platform engineering** — track configuration drift across a fleet of clusters: who/what changed a resource, and what changed exactly (via the stored diff).
- **SecOps** — an independent, operator-owned record of resource state that doesn't depend on the API server's own audit log retention window.
- **Compliance / change auditing** — a durable, timestamped history of every workload state change, per cluster, for retrospective review.

## Key Features

- **Workqueue data plane with per-key serialization** — object changes are drained from a client-go rate-limiting workqueue by a shared pool of workers (default 8). The workqueue contract — an item is never handed to two workers at once, and re-adds for a pending item coalesce — is what makes the version-gated dedup cache correct at any worker count (Invariant 2), and what lets an update storm on one object cost one write instead of one per event.
- **Asynchronous, non-blocking ClickHouse writes** — every insert is handed off to `CHWriter`, a bounded-queue worker pool with exponential-backoff retries (`backoff/v4`). The pipeline never calls ClickHouse synchronously and returns as soon as a job is enqueued, so a slow or unavailable database never stalls a worker or the informers.
- **Watches that start and stop at runtime** — a pool of self-managed dynamic informers is level-triggered towards the desired-state registry: change notifications are debounced (500ms) and reconciled against the running pool, with a 30s safety tick so a failed start or a kind whose CRD arrives later self-heals. Informers are shared per `(resource, namespace)` across every rule and sink that wants them, and each cached object is stripped of `metadata.managedFields` on the way in (its field managers are preserved as the record's `actors`), which is the informer-memory half of running on massive clusters.
- **Hash-based deduplication** — every object's normalized JSON (with `managedFields`, `resourceVersion`, and `generation` stripped) is SHA-256 hashed; an unchanged hash short-circuits as a no-op, so only genuine state changes reach ClickHouse.
- **Graceful JSON diffing** — `Modified` events store a computed JSON patch (`wI2L/jsondiff`) instead of the full object where possible, falling back to the full state whenever a prior baseline is missing or the diff/marshal step itself fails, rather than dropping the row.
- **Version-gated cache, not a naive map** — the in-memory dedup cache (`hashCache`) assigns a monotonic version per key on every write attempt; a commit or delete only applies if its version is still current, so an out-of-order async write can never clobber a newer one.
- **Duplicate-write-safe delete handling** — the live delete path, the reincarnation (delete-then-recreate) close-out path, and the startup zombie-GC pass all claim through the same `ReserveDelete` primitive (with UID verification), so the same disappearance can never produce two `Deleted` rows.
- **Robust zombie-resource garbage collection** — on startup, a background pass compares ClickHouse's last-known state per object against the live (cache-backed) cluster and emits a `Deleted` row for anything that vanished (or was recreated under a new UID) while the operator was down.
- **Crash/restart resilient, non-blocking startup** — cache warm-up and GC run as a `manager.Runnable` in the background; `mgr.Start()` is never gated on ClickHouse being reachable. While the cache is still warming, cache-miss events are tagged `Snapshot` instead of `Added`, so a slow ClickHouse at boot degrades gracefully instead of re-emitting the whole cluster as a flood of duplicate `Added` rows.
- **Self-healing on write failure** — a terminally-failed async write reverts its optimistic cache entry and re-queues the object's key on the workqueue's rate limiter, rather than silently vanishing until an unrelated future change happens to touch the same object.
- **No hardcoded configuration** — ClickHouse address/credentials/timeouts and the cluster identifier are sourced from flags/environment variables; `CH_PASSWORD` is environment-only (never a flag) so it never shows up in a process listing. The set of watched resource types is moving to the `kubestream.io/v1alpha1` CRDs (see [Status](#status)).

## Architecture Overview

```
desired-state registry (watch targets, written by the control plane)
        │  (level-triggered: debounced diff, plus a 30s safety tick)
        ▼
  WatchManager: one self-managed informer per watched (resource, namespace)
    ├─ transform: harvest actors from managedFields, then drop managedFields
    ├─ interest map: (resource, namespace) → interested sinks + their selectors
    └─ selector filtering handler-side, so a selector edit restarts nothing
        │  (Added/Modified/Deleted events → one key per interested sink)
        ▼
  workqueue of identity keys {sink, group, kind, namespace, name}
        │  (rate-limiting; same key never on two workers at once)
        ▼
  pipeline.Process()  ×N workers
    ├─ read the object from the watch cache (no API round-trip)
    ├─ scope no longer watched? → drop (counted, never a Deleted row)
    ├─ normalize object JSON (strip managedFields/resourceVersion/generation/actors annotation)
    ├─ SHA-256 hash + compare against the sink's hashCache (versioned, in-memory)
    ├─ unchanged?  → no-op, return
    ├─ changed?    → compute JSON diff (or full state on cache-miss/diff failure)
    └─ Reserve() a pending cache version, then CHWriter.Enqueue(job)
                       │ (non-blocking, bounded channel)
                       ▼
              CHWriter worker pool
                (backoff/v4 retries per job)
                       │
                       ▼
              ClickHouse INSERT
                       │
          success ─────┴───── failure
            │                    │
   CommitIfCurrent()      revert cache + re-queue the key
   (settle the version)   (UnclaimDelete/AddRateLimited)
```

- **Self-managed informers, one per `(resource, namespace)`**: controller-runtime's cache is scoped once at manager construction and a controller cannot be removed from a running manager, so the watch pool is hand-built: each target gets its own informer, context and goroutine, and the `WatchManager` starts and stops them as rules appear and disappear — no operator restart. Informers are shared across sinks and rules; who wants an event, and under which label selectors, is answered from an in-memory interest map at event time. That is why editing a rule's selector re-filters the stream without re-Listing it, and why two rules on one scope cost one watch.
- **Stopping a watch is not a deletion**: when the last rule wanting a scope goes away, the scope is deregistered first (so in-flight work items for it drop instead of being recorded), then its dedup baselines are evicted, then the informer is stopped and awaited. No `Deleted` rows are written for the objects that were in scope — that distinction is the whole point of scope epochs.
- **Informer caches, not live API calls**: the pipeline has no Kubernetes client at all — it reads object state through a lister interface backed by the watch caches, so nothing in the hot path can issue a direct, uncached API request (Invariant 1, enforced by construction).
- **One dedup cache per sink**: `hashCache` is per-sink and spans every kind that sink receives, because dedup and version state must be independent when one object streams to two sinks — a write confirmed on one says nothing about the other.
- **One `CHWriter` per sink, shared across every watched resource type**: registered as a `manager.Runnable`, its own lifecycle (start workers → drain the queue on shutdown → close the ClickHouse connection) is tied to the manager's. `CHWriter` lives in [`internal/sink/clickhouse`](internal/sink/clickhouse) and implements the backend-agnostic `sink.Writer`, `sink.ScopeEventWriter` (watch-scope epoch rows, on their own small batcher so an audit-critical transition never queues behind a backlog of object rows) and `sink.StateReader` contracts ([`internal/sink`](internal/sink)); the pipeline depends only on those interfaces, so a future backend is a new implementation rather than a change to the hot path.
- **`hashCache` internals**: a mutex-protected map from the canonical identity key `"<group>|<Kind>|<namespace>/<name>"` — version-agnostic, per Invariant 7 — to a versioned `CacheEntry` (hash, compressed normalized JSON, UID, version, pending-delete flag). All commits/deletes are gated on the version issued when the corresponding write was reserved, and the key's shape is what lets a stopped watch scope be evicted by prefix.
- **Warm-up + GC, per watch scope**: warm-up queries the sink for the latest known state of every object in one scope, seeds that scope's dedup cache (without clobbering anything a live work item already wrote), waits for the scope's informer to finish its initial List, and only then diffs the seeded snapshot against the live cluster to close out "zombie" objects that disappeared while the operator was offline. It is per-scope and incremental: a rule created hours after boot warms only what it added, and touches no other scope's history or cache. Until a scope has been warmed, a cache-miss in it is recorded as `Snapshot` rather than `Added`.
- **Scope epochs**: "we stopped watching X" and "X was deleted" are different truths, so they leave different traces. Watch-scope transitions are recorded in `watch_scopes` — one `Started` row when a `(sink, scope)` pair gains its first interested rule, one `Stopped` when it loses its last — and a zombie is only closed out if that log shows the scope was genuinely watched in a previous epoch. Deleting a rule therefore yields one `Stopped` row and **no** `Deleted` rows for the objects it covered; a rule deleted while the operator was down is reconciled the same way on the next start. See [`docs/SCHEMA.md`](docs/SCHEMA.md#watch_scopes).
- **ClickHouse schema v1 is shipped in this repository** — the DDL lives under [`deploy/clickhouse/schema/`](deploy/clickhouse/schema/) and every column is documented in [`docs/SCHEMA.md`](docs/SCHEMA.md).

### ClickHouse schema

`kubestream` writes the per-object change stream to `resource_states` and the
watch-scope epoch log to `watch_scopes`. The full, authoritative DDL is shipped
in-repo and documented column-by-column:

- **DDL:** [`deploy/clickhouse/schema/001_resource_states.sql`](deploy/clickhouse/schema/001_resource_states.sql), [`deploy/clickhouse/schema/002_watch_scopes.sql`](deploy/clickhouse/schema/002_watch_scopes.sql)
- **Reference:** [`docs/SCHEMA.md`](docs/SCHEMA.md) — column semantics, the `event_type` state machine, the RFC 6902 diff format, the version-agnostic identity rule, and a suggested (optional) `TTL` clause.

Apply the two `.sql` files yourself, or start the operator with
`--ch-auto-create-schema=true` to have it execute the shipped DDL idempotently
at connect time. Either way, on connect the operator introspects
`system.columns` and validates the live tables against schema v1; a mismatch is
logged and degrades the `clickhouse-schema` readiness probe (it does not
crash-loop).

## Custom Resources (`kubestream.io/v1alpha1`)

Three CRDs express the two-tier model: a **sink** says *where* state goes, a **rule** says *what* to stream there. They are validated entirely by CRD structural schemas and CEL (`x-kubernetes-validations`) — kubestream registers no admission webhooks and needs no cert-manager.

> **Status:** the types, CRDs, samples, and the reconcilers that consume them all ship now, but nothing constructs the reconcilers yet — `cmd/main.go` is wired last in Phase 1. Until then, applying a rule has no runtime effect — see [Status](#status).

| Kind | Scope | Purpose |
|---|---|---|
| `ClickHouseSink` | Cluster | One ClickHouse instance the operator may write to: connection, per-sink write-path tuning, and an admission policy over what may be written to it. The password lives in a Secret, never in the CR. |
| `StreamRule` | Namespaced | Streams the resources it names **in its own namespace** to a sink. Delegable: a team can opt their own workloads into the audit trail without cluster-level privileges. Naming a cluster-scoped kind here is invalid. |
| `ClusterStreamRule` | Cluster | A `StreamRule` spec plus `spec.namespaceSelector` (nil = all namespaces, including ones created later). The only type permitted to name cluster-scoped kinds. |

Install them with `make install`; they are also part of `make deploy` / `make build-installer`. Working examples live in [`config/samples/`](config/samples/).

```yaml
apiVersion: kubestream.io/v1alpha1
kind: StreamRule
metadata: {name: team-payments-workloads, namespace: payments}
spec:
  sinkRef: default          # immutable — see below
  resources:
  - {group: apps, version: v1, kind: Deployment}
  - group: ""
    version: v1
    kind: ConfigMap
    labelSelector: {matchLabels: {kubestream.io/audit: "true"}}
```

**Validation worth knowing about**

- `kind` must be the Kind (`Deployment`), not the plural resource (`deployments`); `version` must look like `v1` / `v2beta1`; `group` must be empty (core) or a DNS-1123 subdomain. `resources` must be non-empty.
- `spec.sinkRef` defaults to `default` and is **immutable**. Moving a rule to another sink is delete + recreate: re-pointing a live rule would strand the dedup/diff baseline the pipeline built for every object in scope, so records would either re-emit as duplicates or be written as diffs against a baseline the new sink never received. Recreating re-warms the cache from the new sink's own history instead.
- `spec.connection.credentialsSecretRef.namespace` defaults to the **operator's own namespace**, and that default is a security boundary rather than a convenience: the operator's Secret read grant is a namespaced `Role` in that namespace only, so a cluster-scoped sink cannot be used to make it read a Secret its RBAC never intended to expose. The Secret must carry the password under the key `password`; one that exists without a non-empty `password` reports `CredentialsResolved=False/SecretKeyMissing` rather than silently authenticating as nobody.
- `spec.policy.allowedGVKs` restricts what a sink accepts; `kinds: ["*"]` means every kind in the group and the list rejects duplicates server-side. Omitting `allowedGVKs` allows everything **except** the hard deny-list — `v1/Secret` is never watchable in `v1alpha1`, and no policy can re-enable it.
- Writer knobs are bounded: `workers` in `[1, 64]`, `batchMaxRows` in `[1, 100000]`. Omitted fields default to the same values as the `--writer-*` flags below.

`kubectl get` renders `READY`, `SINK`, `WATCHES`, `AGE` for both rule kinds, and `READY`, `ADDR`, `AGE` for sinks. `ADDR` shows the full `host:port`: CRD printer columns are plain JSONPath with no string functions, so the host cannot be split out declaratively.

### Status conditions

Every CR reports why it is or is not working through `metav1.Condition`s carrying `observedGeneration`, so `kubectl describe` is the primary debugging surface — a rule that cannot run degrades **only itself**, and the process never exits over one bad rule. `Ready` is a roll-up: it is non-`True` whenever any specific condition below is, and it carries that condition's own reason forward. Every *transition* of `Ready` also emits an event (`Warning` on a degrade, `Normal` on recovery) — transitions only, so a permanently degraded rule does not flood the namespace's event log on every resync.

**`ClickHouseSink`**

| Condition | Why it is not `True` |
|---|---|
| `CredentialsResolved` | `SecretNotFound` (missing, or outside the namespace the operator may read) or `SecretKeyMissing` (present, but no non-empty `password`). A sink that cannot authenticate is never handed to the sink runtime at all. |
| `SchemaValid` | `SchemaInvalid` — the backend answered and its columns are not the ones this build writes, so it needs a migration. `Unreachable` and `ProbePending` are reported as **`Unknown`**: a host that did not answer has told us nothing about its schema. |
| `Ready` | Rolls the two up. `Unreachable` rolls up to `False` (the sink certainly cannot be written to) while `ProbePending` stays `Unknown`. |

All three come from probes the sink runtime runs on **its own** goroutines; no reconciler ever dials ClickHouse (Invariant 1), so an unreachable sink costs a condition and nothing else.

**`StreamRule` / `ClusterStreamRule`**

| Condition | Why it is not `True` |
|---|---|
| `PolicyAllowed` | `SecretsDenied` (the rule names `v1/Secret`, denied in code — no sink policy can admit it) or `NotInAllowList` (outside a non-empty `spec.policy.allowedGVKs`). A refused rule contributes **nothing** to the watch plan: the refusal is enforced, not merely reported. |
| `ResourceResolved` | `KindsUnresolved`, with a per-kind message: a kind whose CRD is not installed **yet** (self-heals, no restart), or a cluster-scoped kind named by a namespaced `StreamRule` (permanent until the rule is edited — use a `ClusterStreamRule`). |
| `RBACGranted` | `MissingPermissions`, naming the resource, which of `get`/`list`/`watch` are missing, and the scope. The operator can never self-escalate: an administrator adds the grant and the rule activates on its own within one resync (~2m), no restart. `AccessReviewFailed` is `Unknown` — the review itself did not complete, which is not a verdict about the rule. |
| `Ready` | Rolls the three up, and additionally reports `SinkMissing` (its `sinkRef` names no sink — targets are withdrawn) or `SinkNotReady` (the sink exists but is unhealthy — targets are **kept**, see below). |

Failures are per-target wherever they can be: a rule naming five kinds, one of which is not installed, streams the other four and says so in `ResourceResolved`. Only the sink-level and policy-level verdicts are all-or-nothing, because they are statements about the rule as a whole. `status.activeWatches` is read back out of the desired-state registry, so it counts the `(sink, kind, namespace)` scopes the rule actually contributes — a rule can be `Ready` with `activeWatches: 0` when its `namespaceSelector` currently matches no namespace.

A degraded rule normally withdraws its watch targets; **`SinkNotReady` deliberately does not**. An unreachable database is the failure the pipeline's requeue path exists to absorb, and tearing down every watch over a probe blip would evict every dedup baseline the sink serves (forcing a full re-emission on recovery) and write a pair of false scope epochs per scope — which the sink could not even accept, being the thing that is down. The rule reports `Ready=False/SinkNotReady` and keeps streaming into the retrying pipeline.

Neither CRD uses **finalizers**. There is nothing outside the process to clean up (the registry is in-memory state that dies with it), a rule deleted while the operator was down is reconciled by the level-triggered boot pass, and a finalizer would add a failure mode worth more than it buys: a rule stuck `Terminating` because the operator that must release it is not running.

## Configuration

Every setting is available as both a CLI flag and an environment variable (flag wins if both are set), except `CH_PASSWORD`, which is environment-only.

| Flag | Env Var | Default | Description |
|---|---|---|---|
| `--ch-addr` | `CH_ADDR` | `127.0.0.1:9000` | ClickHouse server address (`host:port`, native protocol). |
| `--ch-database` | `CH_DATABASE` | `kubestream` | ClickHouse database name. |
| `--ch-username` | `CH_USERNAME` | `default` | ClickHouse username. |
| — | `CH_PASSWORD` | *(empty)* | ClickHouse password. Env-only by design — flag values are visible in `ps`; connects passwordless if unset (logged as a warning). |
| `--ch-dial-timeout` | `CH_DIAL_TIMEOUT` | `5s` | Timeout for establishing the ClickHouse connection. |
| `--ch-read-timeout` | `CH_READ_TIMEOUT` | `10s` | Timeout for a single ClickHouse query/insert round-trip; also governs the async writer's per-attempt insert timeout. |
| `--ch-auto-create-schema` | `CH_AUTO_CREATE_SCHEMA` | `false` | Execute the shipped DDL (`deploy/clickhouse/schema`) idempotently at connect time. Off by default — the operator never mutates ClickHouse DDL unless asked. |
| `--cluster-id` | `CLUSTER_ID` | `local-kind-cluster` | Identifier for this cluster, recorded on every row. |
| `--writer-queue-size` | `WRITER_QUEUE_SIZE` | `5000` | Capacity (jobs) of the async write hand-off queue. |
| `--writer-workers` | `WRITER_WORKERS` | `4` | Number of workers draining the write queue into ClickHouse. |
| `--writer-batch-max-rows` | `WRITER_BATCH_MAX_ROWS` | `1000` | Row count at which a worker flushes its accumulated insert batch. |
| `--writer-batch-max-wait` | `WRITER_BATCH_MAX_WAIT` | `1s` | Max time a batch's first job waits for the batch to fill before flushing regardless. |
| `--writer-enqueue-timeout` | `WRITER_ENQUEUE_TIMEOUT` | `2s` | How long `Enqueue` waits for queue room before returning an error (the job is never dropped silently). |
| `--writer-drain-timeout` | `WRITER_DRAIN_TIMEOUT` | `15s` | Time budget for draining queued writes to ClickHouse during graceful shutdown. |
| `--metrics-bind-address` | — | `0` (disabled) | Metrics endpoint bind address; `:8443` for HTTPS, `:8080` for HTTP. |
| `--metrics-secure` | — | `true` | Serve the metrics endpoint over HTTPS. |
| `--health-probe-bind-address` | — | `:8081` | Health/readiness probe bind address. |
| `--leader-elect` | — | `false` | Enable leader election (for multi-replica deployments). |
| `--webhook-cert-path` / `--webhook-cert-name` / `--webhook-cert-key` | — | *(empty)* / `tls.crt` / `tls.key` | Webhook server TLS certificate (unused today — no webhooks are registered — reserved for future use). |
| `--metrics-cert-path` / `--metrics-cert-name` / `--metrics-cert-key` | — | *(empty)* / `tls.crt` / `tls.key` | Metrics server TLS certificate. |
| `--enable-http2` | — | `false` | Enable HTTP/2 on the metrics/webhook servers (disabled by default due to known CVEs). |

`CHWriter`'s queue size, worker count, batching knobs, enqueue timeout, and shutdown drain timeout are configurable via the `--writer-*` flags above (D2: operators must be able to size the write path per environment). The per-attempt retry backoff cap (60s) remains an internal default. Use `make bench-load` to measure the effect of a given tuning against a dockerized ClickHouse (see [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)).

Standard `controller-runtime`/Zap logging flags (`--zap-devel`, `--zap-encoder`, `--zap-log-level`, `--zap-stacktrace-level`, `--zap-time-encoding`) are also available; run the binary with `--help` for the full, exact list.

## Metrics

kubestream registers the following pipeline metrics on controller-runtime's global Prometheus registry, so they are served by the existing metrics endpoint (`--metrics-bind-address`). All names are prefixed `kubestream_`.

Every metric a single sink instance reports carries a `sink=<name>` label naming
the `ClickHouseSink` it belongs to: one operator can run several sinks at once
(Task 1.8), and without the label two writers would overwrite each other's
series. Metrics the shared pipeline owns (`dedup_skips_total`,
`pipeline_dropped_total`) carry no `sink` label, because they describe the
workqueue rather than any one backend. A sink's series are deleted when the sink
is deleted, so an absent backend does not linger as a live-but-idle one.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `kubestream_write_queue_depth` | Gauge | `sink` | Jobs currently buffered in the sink's hand-off queue. |
| `kubestream_write_queue_capacity` | Gauge | `sink` | Maximum jobs the sink's hand-off queue can buffer. |
| `kubestream_writes_total` | Counter | `sink`, `outcome="success"\|"failed"` | Settled write jobs, by sink and outcome. |
| `kubestream_write_latency_seconds` | Histogram | `sink` | Time from a job's first write attempt to its final settle (incl. retries). |
| `kubestream_write_retry_attempts_total` | Counter | `sink` | Write attempts beyond the first (i.e. retries), across all jobs. |
| `kubestream_write_batch_rows` | Histogram | `sink` | Rows in each flushed insert batch — the direct signal of how well the batcher is coalescing. |
| `kubestream_enqueue_block_seconds` | Histogram | `sink` | Time `Enqueue` spent blocked waiting for queue room. |
| `kubestream_enqueue_timeouts_total` | Counter | `sink` | `Enqueue` calls that gave up because the queue stayed full past the timeout. |
| `kubestream_dedup_skips_total` | Counter | — | Pipeline work items short-circuited because the object's hash was unchanged. |
| `kubestream_hashcache_entries` | Gauge | `sink` | Live `hashCache` entries, per sink (one cache per sink, spanning all kinds). |
| `kubestream_safe_mode` | Gauge (0/1) | `sink`, `group`, `kind`, `namespace` | `1` while a `(sink, scope)` pair's cache is still warming (Snapshot mode), `0` once warm. |
| `kubestream_pipeline_dropped_total` | Counter | `reason="scope_stopped"` | Work items deliberately discarded — today only because the item's watch scope was deactivated before a worker picked it up. Never a deletion. |

## Getting Started

### Prerequisites

- Go v1.25+ (see `go.mod`)
- Docker (or another `CONTAINER_TOOL`) for building the operator image
- `kubectl`, and access to a Kubernetes cluster
- A reachable ClickHouse instance, with the schema v1 tables created — either apply [`deploy/clickhouse/schema/*.sql`](deploy/clickhouse/schema/) yourself or start the operator with `--ch-auto-create-schema=true` (see [ClickHouse schema](#clickhouse-schema))

`make deploy` installs the [`kubestream.io/v1alpha1` CRDs](#custom-resources-kubestreamiov1alpha1) alongside the operator; `make install` installs the CRDs alone. Which resource types are actually watched is governed by those CRDs once the Phase 1 reconcilers land — see [Status](#status).

### Deploying to a cluster

1. **Build and push the operator image:**

   ```sh
   make docker-build docker-push IMG=<some-registry>/kubestream:tag
   ```

2. **Set the real ClickHouse password.** `config/manager/clickhouse-secret.yaml` ships with a `changeme` placeholder — replace it before applying, e.g.:

   ```sh
   kubectl create secret generic clickhouse-credentials \
     --namespace kubestream-system \
     --from-literal=password='<your-password>' \
     --dry-run=client -o yaml | kubectl apply -f -
   ```

   (For a real deployment, prefer a Kustomize `SecretGenerator` overlay or a secret-management tool — Sealed Secrets, External Secrets, SOPS — over committing a plaintext value.)

3. **Point the deployment at your ClickHouse instance and cluster identifier.** Edit the `env` block in `config/manager/manager.yaml` (`CH_ADDR`, `CH_DATABASE`, `CH_USERNAME`, `CLUSTER_ID`, etc.) — the checked-in values are local/dev placeholders.

4. **Deploy:**

   ```sh
   make deploy IMG=<some-registry>/kubestream:tag
   ```

### Uninstalling

```sh
make undeploy   # operator + CRDs
make uninstall  # CRDs only
```

## Local Development

This project is scaffolded with [Kubebuilder](https://book.kubebuilder.io/) and uses its standard Makefile targets:

```sh
make build          # go build the manager binary
make run            # run the controller locally against your current kubeconfig context
make test           # run the unit/envtest suite (requires the envtest/etcd binaries; make setup-envtest fetches them)
make lint           # run golangci-lint (see .golangci.yml for the enabled linters)
make lint-fix       # run golangci-lint with --fix
make fmt vet        # gofmt + go vet
```

`make test` runs both the pure-Go unit tests (e.g. `internal/pipeline/`, `internal/plan/`, `cmd/main_test.go`) and the Ginkgo/envtest-based CRD validation suite in `api/v1alpha1/`, which spins up a real (test-only) API server via `envtest` — no live cluster is required for it, but the envtest binaries must be present locally (`make setup-envtest`). The pipeline's own suite deliberately needs neither an API server nor a database: it drives the workqueue through in-package fakes for the watch cache and the sink.

Run `make help` for the full list of available targets (image building, Kustomize install/deploy, dependency downloads, etc.).

## License

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
