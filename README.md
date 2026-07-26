# kubestream

**Git blame for your Kubernetes cluster.** A lightweight operator that streams every Pod, Service, and Deployment state change into ClickHouse — so "what did this look like before it broke?" has an answer.

## Overview & Motivation

Kubernetes' own audit trail is short-lived and symptom-focused. `kubectl get events` shows you the last hour or so of what happened, native audit logs capture *API calls* rather than *resulting object state*, and once a Pod is evicted or a Deployment is rolled back, the exact spec/status that existed five minutes before the incident is gone. Incident post-mortems end up reconstructing history from a mix of memory, dashboards, and luck.

`kubestream` is a Kubernetes operator (built on `controller-runtime`/`client-go`) that watches a configurable set of resource types and writes every observed state transition — `Added`, `Modified`, `Deleted` — into a ClickHouse table as an immutable, append-only row. Because it hashes and diffs each object's normalized JSON, it only writes when something actually changed, giving you a compact, queryable, retrospective timeline of cluster state instead of a live-only snapshot.

The watched set is never a compiled-in list: it is declarative runtime configuration, so extending coverage to other built-in types or CRDs (plus a matching RBAC grant) is a configuration change, not a code change.

### Status

The operator is mid-migration to a two-tier, CRD-driven architecture (`ClickHouseSink`, `StreamRule`, `ClusterStreamRule`). The data plane has already been rebuilt as a workqueue pipeline (`internal/pipeline`), replacing the per-GVK controller-runtime reconcilers and the environment-variable GVK list that configured them (both deleted — there is no compatibility shim). The dynamic watch manager, multi-sink runtime, and control-plane reconcilers that drive the pipeline from those CRDs are the remaining Phase 1 work; until they land, the operator starts healthy, serves metrics and probes, and streams nothing.

One consequence worth calling out for anyone deploying from this tree: the operator's base `ClusterRole` (`config/rbac/role.yaml`) is now **empty**. It was generated from the deleted reconcilers' `+kubebuilder:rbac` markers, and standing read access to Pods, Services and Deployments would outlive the code that used it. Leader election and metrics authz are unaffected; the base role and the aggregated `kubestream-watcher` role are rebuilt later in Phase 1.

## Use Cases

- **Incident post-mortems** — "what was this Deployment's spec/status at 03:14 UTC, right before the outage started?" Query ClickHouse instead of hoping someone took a screenshot.
- **Platform engineering** — track configuration drift across a fleet of clusters: who/what changed a resource, and what changed exactly (via the stored diff).
- **SecOps** — an independent, operator-owned record of resource state that doesn't depend on the API server's own audit log retention window.
- **Compliance / change auditing** — a durable, timestamped history of every workload state change, per cluster, for retrospective review.

## Key Features

- **Workqueue data plane with per-key serialization** — object changes are drained from a client-go rate-limiting workqueue by a shared pool of workers (default 8). The workqueue contract — an item is never handed to two workers at once, and re-adds for a pending item coalesce — is what makes the version-gated dedup cache correct at any worker count (Invariant 2), and what lets an update storm on one object cost one write instead of one per event.
- **Asynchronous, non-blocking ClickHouse writes** — every insert is handed off to `CHWriter`, a bounded-queue worker pool with exponential-backoff retries (`backoff/v4`). The pipeline never calls ClickHouse synchronously and returns as soon as a job is enqueued, so a slow or unavailable database never stalls a worker or the informers.
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
Informer caches (one per watched (resource, namespace))
        │  (Added/Modified/Deleted events)
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

- **Informer caches, not live API calls**: the pipeline has no Kubernetes client at all — it reads object state through a lister interface backed by the watch caches, so nothing in the hot path can issue a direct, uncached API request (Invariant 1, enforced by construction).
- **One dedup cache per sink**: `hashCache` is per-sink and spans every kind that sink receives, because dedup and version state must be independent when one object streams to two sinks — a write confirmed on one says nothing about the other.
- **One `CHWriter` per sink, shared across every watched resource type**: registered as a `manager.Runnable`, its own lifecycle (start workers → drain the queue on shutdown → close the ClickHouse connection) is tied to the manager's. `CHWriter` lives in [`internal/sink/clickhouse`](internal/sink/clickhouse) and implements the backend-agnostic `sink.Writer` and `sink.StateReader` contracts ([`internal/sink`](internal/sink)); the pipeline depends only on those interfaces, so a future backend is a new implementation rather than a change to the hot path.
- **One `hashCache` per GVK**: a mutex-protected map from `"<Kind>/<namespace>/<name>"` to a versioned `CacheEntry` (hash, normalized JSON, UID, version, pending-delete flag). All commits/deletes are gated on the version issued when the corresponding write was reserved.
- **Warm-up + GC**: cache warm-up queries ClickHouse for the latest known state of every object in a watch scope, seeds the dedup cache (without clobbering anything a live work item already wrote), then diffs that snapshot against the live cluster to detect and close out "zombie" objects that disappeared while the operator was offline. Until a scope has been warmed, a cache-miss in it is recorded as `Snapshot` rather than `Added`. Re-warming per scope — so a rule created hours after boot warms only what it added — is the next Phase 1 task; the boot-time pass it replaces was removed with the reconcilers it belonged to.
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

> **Status:** the types, CRDs and samples ship now; the reconcilers and the dynamic watch pool that consume them land later in Phase 1. Until then, applying a rule has no runtime effect — see [Status](#status).

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
- `spec.connection.credentialsSecretRef.namespace` defaults to the **operator's own namespace**, and that default is a security boundary rather than a convenience: the operator's ClusterRole grants Secret reads only there, so a cluster-scoped sink cannot be used to make it read a Secret its RBAC never intended to expose.
- `spec.policy.allowedGVKs` restricts what a sink accepts; `kinds: ["*"]` means every kind in the group and the list rejects duplicates server-side. Omitting `allowedGVKs` allows everything **except** the hard deny-list — `v1/Secret` is never watchable in `v1alpha1`, and no policy can re-enable it.
- Writer knobs are bounded: `workers` in `[1, 64]`, `batchMaxRows` in `[1, 100000]`. Omitted fields default to the same values as the `--writer-*` flags below.

`kubectl get` renders `READY`, `SINK`, `WATCHES`, `AGE` for both rule kinds, and `READY`, `ADDR`, `AGE` for sinks. `ADDR` shows the full `host:port`: CRD printer columns are plain JSONPath with no string functions, so the host cannot be split out declaratively.

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

| Metric | Type | Labels | Description |
|---|---|---|---|
| `kubestream_write_queue_depth` | Gauge | — | Jobs currently buffered in the `CHWriter` hand-off queue. |
| `kubestream_write_queue_capacity` | Gauge | — | Maximum jobs the hand-off queue can buffer. |
| `kubestream_writes_total` | Counter | `outcome="success"\|"failed"` | Settled ClickHouse write jobs, by outcome. |
| `kubestream_write_latency_seconds` | Histogram | — | Time from a job's first write attempt to its final settle (incl. retries). |
| `kubestream_write_retry_attempts_total` | Counter | — | Write attempts beyond the first (i.e. retries), across all jobs. |
| `kubestream_enqueue_block_seconds` | Histogram | — | Time `Enqueue` spent blocked waiting for queue room. |
| `kubestream_enqueue_timeouts_total` | Counter | — | `Enqueue` calls that gave up because the queue stayed full past the timeout. |
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
