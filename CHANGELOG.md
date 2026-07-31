# Changelog

All notable changes to kubestream are recorded here. The project is pre-1.0 and
follows [Semantic Versioning](https://semver.org/) loosely: while the API group
is `v1alpha1`, breaking changes are allowed and are always spelled out below.

## Unreleased — Phase 2: proving the foundation at both extremes

### Added

- **A Helm chart** (`deploy/charts/kubestream`) and a **committed, versioned
  `dist/install.yaml`**, alongside the existing kustomize path. All three install
  *the same operator*: the chart renders the same object names as
  `kustomize build config/default`, and `test/chart` asserts that object by
  object — every Role, ClusterRole and binding compared rule for rule, plus the
  manager's arguments, environment, probes and security contexts. That is what
  lets the Phase 1 acceptance suite run against any of them unmodified:
  `E2E_INSTALL=kustomize|helm|installer` changes how the operator is installed and
  nothing else, and `make test-e2e-helm` / `make test-e2e-installer` run the
  lifecycle scenario against the two new paths (both in CI, on their own Kind
  clusters).
  Chart values cover the image (tag or digest), resources, replicas and leader
  election, the metrics endpoint, **which watch presets to install** (a boolean
  per preset, `core-workloads` on by default), extra labels, arbitrary manager
  flags and environment, pod placement, and an optional starter `ClickHouseSink`
  behind `createDefaultSink` (default `false`). The chart **never** creates or
  templates a Secret — a password given as a value would live in the release's
  stored manifest and in every `helm get values` — so sink credentials remain the
  installer's to create. Every value is documented in
  `deploy/charts/kubestream/README.md`.
  The chart's `crds/` and `files/presets/` are generated copies of
  `config/crd/bases/` and `config/rbac/presets/` (Helm requires the first inside
  the chart; the preset templates read the second at render time).
  `make helm-sync` refreshes them and a test fails if they are stale, so a preset
  cannot mean two different things depending on how you installed.
- Packaging targets: `make helm-lint` (`helm lint --strict`, default values and
  both `ci/` values files), `make helm-kubeconform` (`helm template` piped through
  kubeconform at the Kubernetes version the repo already pins for envtest),
  `make installer-kubeconform`, `make helm-template`, and `make verify-packaging`
  which runs the lot. `helm` and `kubeconform` are bootstrapped into `bin/` by the
  Makefile like every other tool, so `make test` — which now runs the chart tests
  — needs nothing provisioned.

- **`Checkpoint` rows** — the `event_type` the schema reserved now ships. A
  `Checkpoint` is a `Modified` that also carries the full state, so
  reconstructing "what did this object look like at time T?" replays a bounded
  number of diffs instead of walking back to the object's creation. It is
  written every `spec.writer.checkpointEvery` consecutive diff-only `Modified`
  rows for an object (new per-sink field, default `50`, `0` disables, bounded
  `[0, 10000]`), and additionally whenever a single diff comes out larger than
  the object it describes — a diff that big costs more than the state it
  replaces. Aggregate reads are unaffected (`Checkpoint` carries the same
  identity, `uid` and `sha256` a `Modified` would have); only a replay treats it
  specially, as its base. The cadence counter lives in the operator's memory and
  resets on restart, which costs nothing because a restart re-baselines with a
  full-state row anyway.
- `docs/SCHEMA.md` publishes the **official state-reconstruction recipe** (read
  with `FINAL` → newest `data`-bearing row → replay subsequent RFC 6902 patches →
  verify against `sha256`), executed and asserted byte-for-byte against a live
  object by a new integration test.
- `pipeline.NormalizedJSON`, the canonical normalized JSON of an object, exported
  for the same reason `pipeline.ObjectHash` was: so an acceptance suite compares
  against what the write path actually produced instead of reimplementing the
  normalization rules and drifting from them.

- A chaos / failure-mode suite (`make test-chaos`) that runs the operator on its
  own Kind cluster against a ClickHouse it stops and starts, and kills the
  operator itself. One scenario per failure mode — backend down at boot, a
  mid-stream outage longer than the writer's retry budget, hand-off-queue
  saturation, a poison row that fails its batch, and a `SIGKILL` mid-flight
  followed by offline deletions — each asserting through direct ClickHouse
  queries *and* the operator's `/metrics` endpoint, plus a standing invariant
  that no object's deletion is ever recorded twice. `make deploy-chaos` /
  `make undeploy-chaos` install the operator with the suite's overlay.
- `pipeline.ObjectHash`, the canonical content hash of an object, exported so an
  acceptance suite can recompute what the write path put in the `sha256` column
  instead of reimplementing the normalization rules and silently drifting from
  them.
- `test/harness`, the acceptance vocabulary (kubectl wrappers, ClickHouse
  querying, condition and pod decoding, manifest rendering, metrics scraping and
  parsing) now shared by `test/e2e` and `test/chaos`, so both suites read the
  sink through one definition of what a row means.

### Changed

- `test/utils.GetProjectDir` finds the module root by walking up to `go.mod`
  rather than stripping the literal `/test/e2e` from the working directory, which
  resolved to the wrong place for any suite outside that one directory.
- `make build-installer` applies its image override through a throwaway overlay in
  `dist/` instead of `kustomize edit set image` inside `config/manager`. Editing
  the committed base rewrote the manager image's *name* there, and both the e2e and
  the chaos overlays select that image by the name `controller` — so building an
  installer for a real registry would have silently stopped those overlays from
  matching, leaving each suite to run whatever image the base then pinned. The
  target's output is unchanged, and it no longer dirties the working tree.

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
- **A delete-and-recreate that happened while the operator was down recorded no
  `Deleted` row for the old UID.** The successor was recorded correctly and a
  plain offline deletion still yielded exactly one `Deleted`, but the old
  incarnation's death never reached the audit trail. Neither half of the
  mechanism was wrong: the live reincarnation branch needs the dedup cache to
  already hold the old UID (after a restart it never does — warm-up waits on
  ClickHouse while the informer only has to reach the API server), and the zombie
  GC's UID-gated claim is *supposed* to be refused for a key the live successor
  already holds.

  The evidence is now taken from where it actually survives — the sink's own
  history. `LastKnownStates` groups per `(namespace, name, uid)` instead of per
  identity, so an identity whose history holds an incarnation that is not the
  newest and has no `Deleted` row of its own is, by definition, a death nobody
  recorded. The warm-up seeds the newest incarnation exactly as before and emits
  one `Deleted` row for each unclosed prior, behind the same scope-epoch gate the
  zombie GC sits behind (pre-history is never fabricated into a deletion).

  Which incarnation history holds when the warm reads it depends on a race the
  operator does not control — whether the successor's own first row reached the
  sink before that read — so both outcomes are handled. If the successor's row
  had not landed, the old incarnation is seeded and swept like any other object,
  the sweep finds a different UID live under its name, and its UID-gated claim is
  refused (correctly: the cache entry belongs to the successor). That refusal is
  now recorded rather than dropped: the pass waits, bounded, for history to catch
  up and then closes the old UID out from it. No GC decision changed — the
  refusal still stands and the live entry is never claimed.

  The recovered row is **dated from history**, never from the recovery. That
  keeps reconstruction in the order events actually happened (`Added` → `Deleted`
  for the old UID → the successor's first row, rather than a close-out that sorts
  after the successor and buries it), and it makes every field of the row a
  function of history — so a re-emitted close-out is byte-identical and is
  collapsed on merge by `resource_states`' `ReplacingMergeTree`. Once the row
  lands, that UID's own latest event is `Deleted` and it is excluded from every
  later warm-up, so the recovery is self-limiting.
- **A deleted-and-re-created `ClickHouseSink` was never boot-reconciled again.**
  `Pipeline.RemoveSink` discarded the sink's dedup caches, but the warm/GC
  coordinator kept it marked as boot-reconciled forever, so the pass that writes
  `Stopped` rows for scopes an earlier process left open never ran for that name
  again and scopes orphaned during the sink's absence stayed open in
  `watch_scopes` indefinitely. The sink runtime now clears that bookkeeping
  (`WarmCoordinator.ForgetSink`) immediately alongside the pipeline eviction,
  cancelling any warm still in flight for the sink at the same time.

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
