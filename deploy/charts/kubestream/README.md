# kubestream Helm chart

Installs the kubestream operator: the CRDs, its RBAC (including the aggregated
watch role and whichever watch presets you enable), the manager Deployment and,
optionally, a starter `ClickHouseSink`.

This chart is one of two supported install paths. The other is the single-file
`dist/install.yaml` built by `make build-installer`. They install **the same
operator** — the same object names, the same permissions rule for rule, the same
container arguments — and `test/chart` asserts that, object by object, against
`kustomize build config/default`. The project's acceptance suite runs against
either one unmodified (`make test-e2e-helm`, `make test-e2e-installer`).

## Install

```sh
helm install kubestream deploy/charts/kubestream \
  --namespace kubestream-system --create-namespace
```

The chart is packaged and attached to every tagged
[release](https://github.com/yelzhy/kuberecord/releases) alongside a
`checksums.txt`, so a `kubestream-X.Y.Z.tgz` can be installed directly without a
checkout. There is no chart repository. The chart is **not** versioned
independently of the operator: `version` is `X.Y.Z` and `appVersion` is `vX.Y.Z`,
both equal to the operator release it installs — see
[`docs/RELEASING.md`](../../../docs/RELEASING.md).

Use the release name `kubestream`: it matches the chart name, so every object
comes out named exactly as the kustomize install names it
(`kubestream-controller-manager`, `kubestream-metrics-reader`, …). Any other
release name works, but the names gain a prefix and anything referring to them —
a scrape config, a ClusterRoleBinding for metrics readers — has to follow.

The operator boots healthy and **idle**: it streams nothing until a
`ClickHouseSink` and at least one `StreamRule` or `ClusterStreamRule` exist. The
post-install notes walk through those three steps; the project
[README](../../../README.md#installing) covers them in full, and
[`examples/quickstart/`](../../../examples/quickstart/) runs the whole sequence
end to end on a kind cluster.

### Credentials are yours to create

The chart never creates or templates a Secret. A password passed as a Helm value
would be stored in the release's manifest and echoed by `helm get values`, so the
sink's password has to be created out of band, in the operator's namespace — the
only namespace the operator can read Secrets in:

```sh
kubectl create secret generic clickhouse-credentials \
  --namespace kubestream-system --from-literal=password='<your-password>'
```

### Upgrades and CRDs

The CRDs live in `crds/`, per Helm convention. Helm **installs** them and never
upgrades or deletes them, so a chart upgrade that ships changed CRDs needs one
explicit step first:

```sh
kubectl apply -f deploy/charts/kubestream/crds/
helm upgrade kubestream deploy/charts/kubestream --namespace kubestream-system
```

`helm uninstall` likewise leaves the CRDs — and therefore your `ClickHouseSink`
and rules — in place. Deleting them deletes every CR of those kinds; the rows
already in ClickHouse are untouched, since the sink is the durable store
(Invariant 6).

## Values

### Image

| Value | Type | Default | Description |
|---|---|---|---|
| `image.repository` | string | `ghcr.io/yelzhy/kubestream` | Operator image repository. |
| `image.tag` | string | `""` | Image tag. Empty follows the chart's `appVersion`, so upgrading the chart moves the operator with it. |
| `image.digest` | string | `""` | Image digest (`sha256:…`). Set, it wins over `image.tag` — a digest pins by content, a tag only by name. |
| `image.pullPolicy` | string | `IfNotPresent` | Container image pull policy. |
| `imagePullSecrets` | list | `[]` | Pull secrets for a private registry, as `[{name: my-secret}]`. |

### Operator identity and scale

| Value | Type | Default | Description |
|---|---|---|---|
| `clusterID` | string | `local-kind-cluster` | `CLUSTER_ID`: stamped on every row and scope event this operator writes, and part of an object's identity in the schema. **Change it before installing anywhere real** — rows already written keep the old value. Empty leaves the binary's own default in force. |
| `replicaCount` | int | `1` | Manager replicas. More than one buys faster failover, not throughput: the leader owns every informer and the whole pipeline. Requires `leaderElection.enabled=true`; the chart refuses to render otherwise. |
| `leaderElection.enabled` | bool | `true` | Pass `--leader-elect` and install the lease `Role`/`RoleBinding`. What makes `replicaCount: 2` safe, and what stops an operator left behind by a half-finished upgrade from double-writing every row. |
| `resources` | object | `500m`/`128Mi` limits, `10m`/`64Mi` requests | Manager resources, sized for the *small* profile in [`docs/PERFORMANCE.md`](../../../docs/PERFORMANCE.md). Raise them to match the profile you actually run: the `hashCache` and the write queues grow with the watched-object count. |
| `terminationGracePeriodSeconds` | int | `10` | Shutdown grace period. A sink whose `spec.writer.drainTimeout` exceeds this is cut short mid-drain, so raise it if you raise that. |

### Metrics

| Value | Type | Default | Description |
|---|---|---|---|
| `metrics.enabled` | bool | `true` | Serve the metrics endpoint. One switch for four things: the `--metrics-bind-address` argument, the container port, the metrics `Service` and the endpoint's authn/authz ClusterRoles. |
| `metrics.secure` | bool | `true` | HTTPS with request authentication (TokenReview) and authorization (SubjectAccessReview). `false` serves plaintext HTTP to anything that can reach the pod. |
| `metrics.port` | int | `8443` | Port the endpoint binds, and the `Service`'s port and target port. |
| `metrics.service.annotations` | object | `{}` | Annotations for the metrics `Service` (e.g. a scrape hint). |

Scrapers need the `<release>-metrics-reader` ClusterRole, which is shipped
**unbound** on purpose — handing out read access to the metrics is the
installer's decision, not a side effect of installing:

```sh
kubectl create clusterrolebinding kubestream-metrics-reader \
  --clusterrole=kubestream-metrics-reader \
  --serviceaccount=monitoring:prometheus
```

The chart ships no `ServiceMonitor`; `config/prometheus/monitor.yaml` in the
repository is a starting point if you run the Prometheus Operator.

### Health probes

| Value | Type | Default | Description |
|---|---|---|---|
| `healthProbe.port` | int | `8081` | Port for `/healthz` and `/readyz`, and the container's `health` port. Both probes are plain pings: a cluster with no sinks is healthy, and an unreachable ClickHouse is reported as a condition on the sink's CR rather than as process unreadiness — which would take the pod out of service and stop every *other* sink with it. |

### RBAC and watch presets

| Value | Type | Default | Description |
|---|---|---|---|
| `rbac.create` | bool | `true` | Install every `Role`, `ClusterRole` and binding. `false` only when a platform team manages them out of band: with nothing bound, the operator cannot read even its own CRs. |
| `rbac.presets.core-workloads` | bool | `true` | Watch `pods`, `services`, `configmaps`, `serviceaccounts` and the `apps/v1` workload kinds. Enabled by default because it is what the shipped sample rules watch. |
| `rbac.presets.batch` | bool | `false` | Watch `batch` `jobs` and `cronjobs`. |
| `rbac.presets.networking` | bool | `false` | Watch `ingresses`, `ingressclasses`, `networkpolicies`, `endpointslices` and `endpoints`. |
| `rbac.presets.storage` | bool | `false` | Watch `persistentvolumes`, `persistentvolumeclaims`, `storageclasses`, `volumeattachments`, `csidrivers`, `csinodes`. |
| `rbac.presets.rbac-read` | bool | `false` | Watch `roles`, `rolebindings`, `clusterroles`, `clusterrolebindings`. The highest-value audit trail kubestream can produce, and the one preset worth a second thought: it puts the cluster's whole authorization graph in ClickHouse. See [`docs/RBAC.md`](../../../docs/RBAC.md). |

Each enabled preset renders one `ClusterRole` labelled
`kubestream.io/aggregate-to-watcher: "true"`, which the aggregated
`<release>-watcher` role picks up — no restart, no redeploy. The rules come
verbatim from `config/rbac/presets/`, so a Helm-installed preset and a
`kubectl apply -f config/rbac/presets/networking.yaml` grant the same thing.

Two consequences worth stating plainly:

- **A missing grant is data, not an outage.** A rule naming a kind no enabled
  preset covers reports `RBACGranted=False/MissingPermissions` while every other
  rule keeps streaming, and flips to `True` on its own within one resync once the
  grant appears.
- **The operator cannot escalate itself.** It has no write access to
  `clusterroles` or `clusterrolebindings`, and an administrator applying a preset
  is still bound by Kubernetes' own escalation check.

For a kind no preset covers (a CRD, typically), copy any preset, change the rules
and keep the label — `helm upgrade` is not involved. Never grant `secrets`:
`v1/Secret` is hard-denied as a watchable kind in code (D8), so the grant would be
dead privilege.

### Service account, labels, scheduling

| Value | Type | Default | Description |
|---|---|---|---|
| `serviceAccount.create` | bool | `true` | Create the operator's ServiceAccount. |
| `serviceAccount.name` | string | `""` | Its name. Empty means `<release>-controller-manager`. With `create: false`, this names an existing account to bind to (empty falls back to `default`, which is almost never what you want). |
| `serviceAccount.annotations` | object | `{}` | Annotations, e.g. for cloud workload identity. |
| `extraLabels` | object | `{}` | Labels added to every object the chart renders, including the pod template. Deliberately **not** added to the Deployment's or the metrics Service's selector: a Deployment's selector is immutable, so folding them in would make the next `helm upgrade` fail. |
| `podAnnotations` | object | `{}` | Annotations on the manager pod. |
| `nodeSelector` | object | `{}` | Pod node selector. |
| `tolerations` | list | `[]` | Pod tolerations. |
| `affinity` | object | `{}` | Pod affinity/anti-affinity. |
| `priorityClassName` | string | `""` | Pod priority class. |
| `podSecurityContext` | object | `runAsNonRoot`, `RuntimeDefault` seccomp | Pod-level security context. |
| `containerSecurityContext` | object | read-only root FS, no privilege escalation, all capabilities dropped | Container security context. |

The two security contexts satisfy the `restricted` Pod Security Standard, and the
e2e suite installs this chart into a namespace that *enforces* it — so they are
tested, not aspirational. Relaxing them is supported but is on you.

### Operator flags and environment

| Value | Type | Default | Description |
|---|---|---|---|
| `extraArgs` | list | `[]` | Extra arguments for the manager. This is how the operator's remaining flags are set — they are process tuning, not packaging, so the chart passes them through rather than growing a value per flag. |
| `extraEnv` | list | `[]` | Extra environment entries (standard `env` items). Every flag has an environment twin; the flag wins when both are set. |

```yaml
extraArgs:
  - --ch-auto-create-schema        # let the operator apply deploy/clickhouse/schema/*.sql
  - --pipeline-workers=16          # more workqueue drainers on a large cluster
  - --writer-batch-max-rows=5000   # fleet-wide default for sinks that omit it
```

The full flag list is in
[`docs/CONFIGURATION.md`](../../../docs/CONFIGURATION.md);
`--ch-auto-create-schema` is the one
most installs want to decide about explicitly, since it is what lets the operator
create the schema v1 tables itself instead of you applying the DDL.

### Starter sink (optional)

| Value | Type | Default | Description |
|---|---|---|---|
| `createDefaultSink` | bool | `false` | Render a `ClickHouseSink`. Off by default: a sink needs an address and a password, and the password is yours to create. |
| `defaultSink.name` | string | `default` | The sink's name. `default` is what a rule's `spec.sinkRef` defaults to, so a single-backend install needs no `sinkRef` anywhere. |
| `defaultSink.connection.addr` | string | `""` | `host:port` of ClickHouse's native protocol (9000). **Required** when `createDefaultSink` is true. |
| `defaultSink.connection.database` | string | `kubestream` | Database holding the schema v1 tables. |
| `defaultSink.connection.username` | string | `kubestream` | ClickHouse user. |
| `defaultSink.connection.credentialsSecretRef.name` | string | `clickhouse-credentials` | Secret with a `password` key, resolved in the operator's own namespace. |
| `defaultSink.connection.dialTimeout` | duration | `""` | Dial timeout; empty keeps the CRD default. |
| `defaultSink.connection.readTimeout` | duration | `""` | Read timeout; empty keeps the CRD default. |
| `defaultSink.writer` | object | `{}` | `spec.writer` verbatim (`queueSize`, `workers`, `batchMaxRows`, `batchMaxWait`, `enqueueTimeout`, `drainTimeout`, `checkpointEvery`). Omitted fields keep their CRD defaults. |
| `defaultSink.policy` | object | `{}` | `spec.policy` verbatim (`allowedGVKs`). Omitted admits everything except the hard deny-list. |

Every unset field is *omitted* from the rendered CR rather than written out with a
copy of its default, so the CRD's defaults — and only those — apply, and a future
change to one of them reaches existing installs.

### Naming overrides

| Value | Type | Default | Description |
|---|---|---|---|
| `nameOverride` | string | `""` | Override the chart name in labels and generated names. |
| `fullnameOverride` | string | `""` | Override the generated name prefix entirely. |

## Development

```sh
make helm-sync           # refresh crds/ and files/presets/ from config/ (run after `make manifests`)
make helm-lint           # helm lint --strict, default values and every ci/ values file
make helm-kubeconform    # helm template | kubeconform, against the pinned Kubernetes version
make helm-template       # render to stdout (VALUES=<file> for one of the ci/ files)
go test ./test/chart/    # the template tests, including parity with kustomize build config/default
make test-e2e-helm       # install this chart on kind and run the Phase 1 happy path against it
```

`crds/` and `files/presets/` are **generated copies** of `config/crd/bases/` and
`config/rbac/presets/`. Helm requires the CRDs inside the chart and the preset
templates read those files at render time, so copies are unavoidable — a stale
copy is not: `go test ./test/chart/` compares them byte for byte and names
`make helm-sync` as the fix.

`ci/minimal-values.yaml` and `ci/all-features-values.yaml` are the two extremes
lint and validation run against: nothing optional enabled, and everything at
once.
