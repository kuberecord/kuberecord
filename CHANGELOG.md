# Changelog

All notable changes to kubestream are recorded here. The project is pre-1.0 and
follows [Semantic Versioning](https://semver.org/) loosely: while the API group
is `v1alpha1`, breaking changes are allowed and are always spelled out below.

## Unreleased — Phase 1: the two-tier CRD architecture

### Removed — BREAKING: all environment-variable configuration of *what* and *where*

The operator no longer takes its configuration from the process environment.
There is **no compatibility shim** (D5: the project has no existing users, so a
shim would only preserve a design nobody depends on):

| Removed | Replaced by |
|---|---|
| `WATCHED_GVKS` (and its `parseGVKList` parser) | `StreamRule` / `ClusterStreamRule` custom resources — `spec.resources`, evaluated live |
| `CH_ADDR` / `--ch-addr` | `ClickHouseSink` `spec.connection.addr` |
| `CH_DATABASE` / `--ch-database` | `ClickHouseSink` `spec.connection.database` |
| `CH_USERNAME` / `--ch-username` | `ClickHouseSink` `spec.connection.username` |
| `CH_PASSWORD` | the Secret named by `spec.connection.credentialsSecretRef`, read from the operator's own namespace only |
| `CH_DIAL_TIMEOUT` / `--ch-dial-timeout` | `ClickHouseSink` `spec.connection.dialTimeout` |
| `CH_READ_TIMEOUT` / `--ch-read-timeout` | `ClickHouseSink` `spec.connection.readTimeout` |

A single global ClickHouse connection opened at boot is gone with them: the
operator now runs one backend instance per `ClickHouseSink`, created, recycled
(on credential rotation) and drained at runtime.

The `clickhouse-schema` readiness check is also removed. A cluster with no sinks
is a valid, healthy state, and a schema mismatch is now reported as
`SchemaValid=False` on the affected `ClickHouseSink` rather than as process
unreadiness that would have taken every other sink out of service with it.
`/healthz` and `/readyz` are plain pings.

### Added

- `--operator-namespace` (env `POD_NAMESPACE`, supplied from the downward API in
  the shipped Deployment). **Required**: it is the namespace a sink's
  `credentialsSecretRef` defaults to and the only namespace the operator reads
  Secrets in, so the operator refuses to guess it.
- `--pipeline-workers` (env `PIPELINE_WORKERS`, default 8) — goroutines draining
  the shared data-plane workqueue.
- An end-to-end acceptance suite (`make test-e2e`) that runs the operator on a
  real Kind cluster against a real in-cluster ClickHouse and asserts by querying
  the sink directly: the full create/scale/delete lifecycle of a watched object,
  scope epochs across a rule being deleted and re-created, RBAC degrade and
  self-heal, offline deletions and reincarnations across an operator restart, and
  cluster-scoped kinds. `make deploy-e2e` / `make undeploy-e2e` install the
  operator with the suite's overlay.

### Changed

- `--cluster-id` (env `CLUSTER_ID`) survives unchanged: it identifies *this
  operator instance's* cluster and is stamped on every row and scope event, so it
  is not a property of any one sink.
- The six `--writer-*` flags (env `WRITER_*`) are now **fallbacks**, applied per
  field: a `ClickHouseSink` that sets `spec.writer.<field>` uses its own value,
  one that omits it uses the flag.
- `--ch-auto-create-schema` (env `CH_AUTO_CREATE_SCHEMA`) survives as an
  operator-level setting and now applies to every sink instance, which executes
  the shipped DDL in the background before its first write.
- The e2e suite no longer installs cert-manager. Validation is CEL-only and there
  are no webhooks (D4), so nothing in the suite ever needed it.

### Fixed

- The operator could not reconcile any `ClickHouseSink` under its own shipped
  RBAC. The `SinkReconciler` watches Secrets to close the credential-rotation
  loop, and a watch through the manager's cache lists **cluster-wide** by
  default — but the operator's Secret grant is a namespaced `Role`, deliberately
  (D7). The API server refused the list, the cache never synced, and every sink
  sat unreconciled with an empty status. The manager's cache now confines its
  Secret informer to `--operator-namespace`, so the list it issues is exactly the
  one the Role permits; the grant is unchanged. Found by the new e2e suite —
  envtest cannot reproduce it, because its client is effectively an
  administrator.

### Known gap

- **A delete-and-recreate that happens while the operator is down records no
  `Deleted` row for the old UID.** The successor is recorded under its own UID,
  and a plain offline deletion still yields exactly one `Deleted` — but the
  reincarnation close-out is lost, so the old incarnation's death never reaches
  the audit trail.

  Neither half of the mechanism is individually wrong. The close-out comes from
  the pipeline's reincarnation branch, which needs the dedup cache to already
  hold the old UID — that is, it needs the scope's warm-up from the sink's
  history to finish before the informer's initial list is processed. After a
  restart it never does: warm waits for the sink to dial ClickHouse while the
  informer only has to reach the API server. The new object is seen first and
  tagged `Snapshot`, and the zombie GC then *refuses* to claim the old UID
  because the key is already reserved by the live successor — a refusal that is
  itself a deliberate safeguard against deleting a live object by name alone.

  Fixing it means revising the warm/GC ordering, or letting a refused claim still
  record the old UID's death. Both are changes to that component's design under
  Invariant 3, so this is reported rather than patched from the e2e suite; the
  scenario asserts what the operator actually does and the omission is documented
  inline in `test/e2e/scenarios_test.go`.

### Migration

1. `make install` (or `make deploy`) to install the `kubestream.io/v1alpha1` CRDs.
2. Create the credentials Secret in the operator's namespace
   (`kubectl create secret generic clickhouse-credentials --from-literal=password=…`).
3. Translate the old `CH_*` environment block into a `ClickHouseSink` — name it
   `default` to match the shipped samples' `spec.sinkRef`.
4. Translate the old `WATCHED_GVKS` list into one or more `StreamRule` /
   `ClusterStreamRule` resources, and apply the matching RBAC preset from
   `config/rbac/presets/` so the operator is allowed to watch those kinds.

The full walkthrough is the README's
[Getting Started](README.md#getting-started).
