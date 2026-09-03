# KubeRecord Helm Chart

A Helm chart for deploying the [kuberecord operator](https://github.com/kuberecord/kuberecord)
on Kubernetes, a tool that streams cluster state changes to ClickHouse and to
S3-compatible object storage for audit, GitOps forensics and change intelligence.

Source code can be found here:

<https://github.com/kuberecord/kuberecord>

Installs the kuberecord operator: the CRDs, its RBAC (including the aggregated
watch role and whichever watch presets you enable), the manager Deployment and,
optionally, the sinks you list — `ClickHouseSink`, `S3Sink`, or both.

## Prerequisites

| | |
|---|---|
| **Kubernetes** | 1.29 or newer. Validation is CRD structural schemas plus CEL, which is what sets the floor — the chart installs no webhooks and no cert-manager. |
| **Helm** | 3.8 or newer, for the OCI registry support the install below relies on. |
| **A sink endpoint** | A reachable ClickHouse, or an S3-compatible bucket, or both. Neither has to exist at install time — the operator boots healthy and idle without one. |
| **Cluster-admin, once** | The chart installs CRDs and ClusterRoles. |

## Installation

```sh
helm install kuberecord oci://ghcr.io/kuberecord/charts/kuberecord \
  --version 0.3.1 \
  --namespace kuberecord-system --create-namespace
```

The chart is published to the registry as an OCI artifact from **v0.3.0** onward.
There is no classic chart repository and no `helm repo add` — an OCI reference is
the whole address, and helm has needed no extra configuration to install from one
since v3.8.

**The `--version` is not optional here, and it takes no `v`.** A Helm chart
version is plain semver, so the artifact's tag is `0.3.1` where the operator's is
`v0.3.1`. The two always agree otherwise: the chart is **not** versioned
independently of the operator — `version` is `X.Y.Z` and `appVersion` is `vX.Y.Z`,
both equal to the operator release it installs — see
[`docs/RELEASING.md`](https://github.com/kuberecord/kuberecord/blob/main/docs/RELEASING.md).

The chart in the registry is signed with cosign, and pinning by digest is
supported — [`docs/VERIFYING.md`](https://github.com/kuberecord/kuberecord/blob/main/docs/VERIFYING.md#the-chart-signature)
has both commands.

The operator boots healthy and **idle**: it streams nothing until a sink and at
least one `StreamRule` or `ClusterStreamRule` exist. The post-install notes walk
through those three steps; the
[project README](https://github.com/kuberecord/kuberecord/blob/main/README.md#installing)
covers them in full, and
[`examples/quickstart/`](https://github.com/kuberecord/kuberecord/tree/main/examples/quickstart/)
runs the whole sequence end to end on a kind cluster.

## Upgrades and CRDs

The CRDs live in `crds/`, per Helm convention, which means Helm **installs** them
but never upgrades or deletes them. A chart upgrade that ships changed CRDs
therefore needs one explicit step first — see
[`docs/UPGRADING.md`](https://github.com/kuberecord/kuberecord/blob/main/docs/UPGRADING.md)
for the version-specific instructions:

```sh
# Apply the CRDs from the chart archive, not from a local checkout: they must be
# the ones the version you are upgrading to ships.
helm pull oci://ghcr.io/kuberecord/charts/kuberecord --version 0.3.1 --untar
kubectl apply -f kuberecord/crds/

helm upgrade kuberecord oci://ghcr.io/kuberecord/charts/kuberecord \
  --version 0.3.1 --namespace kuberecord-system
```

`helm uninstall` likewise leaves the CRDs — and therefore your sinks and rules —
in place. Deleting them deletes every CR of those kinds; the rows already in
ClickHouse and the objects already in S3 are untouched, since the sink is the
durable store.

## Credentials are yours to create

The chart never creates or templates a Secret. A credential passed as a Helm value
would be stored in the release's manifest and echoed by `helm get values`, so a
sink's credentials have to be created out of band, in the operator's namespace —
the only namespace the operator can read Secrets in:

```sh
# A ClickHouseSink's password.
kubectl create secret generic clickhouse-credentials \
  --namespace kuberecord-system --from-literal=password='<your-password>'

# An S3Sink's access key — needed for MinIO and on-premises stores. On a cloud
# provider, omit spec.credentials instead and authenticate from the ambient chain
# (IRSA, workload identity, an instance role), so no long-lived key exists at all.
kubectl create secret generic s3-credentials \
  --namespace kuberecord-system \
  --from-literal=accessKeyId='<id>' \
  --from-literal=secretAccessKey='<key>'
```

## Values

Every value has a working default, with one exception worth stating up front:
**`clusterID` is stamped on every row this operator writes and forms part of an
object's identity in the schema, so set it before installing anywhere real** —
rows already written keep the old value. A minimum viable install is one line:

```yaml
# values.yaml — everything else can stay defaulted.
clusterID: prod-eu-west-1
```

Add a sink to it when you are ready to stream somewhere (see
[Sinks](#sinks-optional)); the operator runs healthy and idle until you do.

### Image

| Value | Type | Default | Description |
|---|---|---|---|
| `image.repository` | string | `ghcr.io/kuberecord/kuberecord` | Operator image repository. |
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
| `resources` | object | `500m`/`128Mi` limits, `10m`/`64Mi` requests | Manager resources, sized for the *small* profile in [`docs/PERFORMANCE.md`](https://github.com/kuberecord/kuberecord/blob/main/docs/PERFORMANCE.md). Raise them to match the profile you actually run: the `hashCache` and the write queues grow with the watched-object count. |
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
kubectl create clusterrolebinding kuberecord-metrics-reader \
  --clusterrole=kuberecord-metrics-reader \
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
| `rbac.presets.rbac-read` | bool | `false` | Watch `roles`, `rolebindings`, `clusterroles`, `clusterrolebindings`. The highest-value audit trail kuberecord can produce, and the one preset worth a second thought: it puts the cluster's whole authorization graph in ClickHouse. See [`docs/RBAC.md`](https://github.com/kuberecord/kuberecord/blob/main/docs/RBAC.md). |

Each enabled preset renders one `ClusterRole` labelled
`kuberecord.io/aggregate-to-watcher: "true"`, which the aggregated
`<release>-watcher` role picks up — no restart, no redeploy. The rules come
verbatim from `config/rbac/presets/`, so a Helm-installed preset and a
`kubectl apply -f config/rbac/presets/networking.yaml` grant the same thing.

Two consequences:

- **A missing grant is not an outage.** A rule naming a kind no enabled
  preset covers reports `RBACGranted=False/MissingPermissions` while every other
  rule keeps streaming, and flips to `True` on its own within one resync once the
  grant appears.
- **The operator cannot escalate itself.** It has no write access to
  `clusterroles` or `clusterrolebindings`, and an administrator applying a preset
  is still bound by Kubernetes' own escalation check.

For a kind no preset covers (a CRD, typically), copy any preset, change the rules
and keep the label — `helm upgrade` is not involved. Never grant `secrets`:
`v1/Secret` is hard-denied as a watchable kind in code, so the grant would be
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

The two security contexts satisfy the `restricted` Pod Security Standard.
Relaxing them is supported but is on you.

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
[`docs/CONFIGURATION.md`](https://github.com/kuberecord/kuberecord/blob/main/docs/CONFIGURATION.md);
`--ch-auto-create-schema` is the one
most installs want to decide about explicitly, since it is what lets the operator
create the schema v1 tables itself instead of you applying the DDL.

### Sinks (optional)

| Value | Type | Default | Description |
|---|---|---|---|
| `sinks` | list | `[]` | The sink CRs this release creates. Empty means the chart creates none — apply your own, or add entries here. |
| `sinks[].kind` | string | — | `ClickHouseSink` or `S3Sink`: the sink CRDs this chart ships. Any other kind fails the render. |
| `sinks[].name` | string | — | The sink's name — what a rule names in `spec.sink.name`. A name is unique only *within* a kind, so a `ClickHouseSink` and an `S3Sink` may share one. |
| `sinks[].spec` | object | — | The CR's `spec`, passed through **verbatim**. Every field of the kind's CRD is reachable; see the [`ClickHouseSink`](https://github.com/kuberecord/kuberecord/blob/main/docs/CRDS.md#clickhousesink) and [`S3Sink`](https://github.com/kuberecord/kuberecord/blob/main/docs/CRDS.md#s3sink) condition and field references. |

`spec` is passed through rather than mapped field by field, so every field of the
kind's CRD is reachable and what counts as *valid* is decided by the CRD itself.
An unset field is left out of the rendered CR entirely, so the CRD's own defaults
apply — and a later change to one of them reaches existing installs.

The chart still fails the render on four things, rather than leaving them to
surface later as a confusing API error:

- an unknown `kind` (which would otherwise fail as a missing CRD, not as a typo);
- a missing `name` or `spec`;
- two entries sharing one `(kind, name)` identity;
- the one connection field per backend that cannot be defaulted —
  `spec.connection.addr` and `spec.connection.credentialsSecretRef.name` for a
  `ClickHouseSink`, `spec.bucket` for an `S3Sink`.

**The chart never creates a Secret**, at any values, and no sink spec should carry
a credential inline — a password given as a value lands in the release's stored
manifest and in every `helm get values` output. Create it yourself in the release
namespace, the only namespace the operator can read Secrets in. An `S3Sink` on a
cloud provider needs no Secret at all: omit `spec.credentials` and it
authenticates from the ambient chain (IRSA, workload identity, an instance role).

The two-sink example below is the tee pattern — one queryable ClickHouse
timeline and one immutable S3 archive, fed by two rules over the same resources.
See [`docs/TEE.md`](https://github.com/kuberecord/kuberecord/blob/main/docs/TEE.md).

```yaml
sinks:
  - kind: ClickHouseSink
    name: hot
    spec:
      connection:
        addr: clickhouse.kuberecord-system.svc:9000
        credentialsSecretRef:
          name: clickhouse-credentials
  - kind: S3Sink
    name: cold
    spec:
      bucket: kuberecord-archive
      region: eu-west-1
```

An `S3Sink` is a `Writer`-only archive tier: it reports
`HistoryUnavailable=True` permanently while `Ready` stays `True`, cannot warm its
dedup cache from its own history, and records every object as a full `Snapshot` on
each restart. That is a declared capability limit, not a fault.

### Naming overrides

| Value | Type | Default | Description |
|---|---|---|---|
| `nameOverride` | string | `""` | Override the chart name in labels and generated names. |
| `fullnameOverride` | string | `""` | Override the generated name prefix entirely. |
