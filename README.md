# KubeRecord

**Git blame for your Kubernetes cluster.**

It is 02:47. `checkout` is throwing 5xx. Nobody deployed, and
`kubectl get events` has already forgotten why.

```console
$ kubectl kuberecord timeline deploy/checkout -n payments --with-events
→ discovered ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)
→ cluster-id prod-eu-1 (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
Kind:     apps/Deployment
Object:   payments/checkout
Cluster:  prod-eu-1
UID:      7c9e6679-7425-40de-944b-e07fc1f90ae7
Coverage: 2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)

TIME (UTC)               EVENT     ACTOR                      CHANGE
2026-08-28 14:09:40.900  Modified  unknown                    ~ metadata.…deployment.kubernetes.io/revision: 1 → 2
2026-08-28 14:06:44.020  Event     replicaset-controller      ⚠ FailedCreate: pods "checkout-7d4f-" is forbidden: excee…
2026-08-28 14:05:02.117  Modified  kube-controller-manager    ~3 ops
2026-08-28 14:03:20.310  Event     kube-controller-manager    ScalingReplicaSet: Scaled up replica set checkout-7d4f to…
2026-08-28 14:03:11.482  Modified  kubectl-client-side-apply  ~ spec.…containers[0].resources.limits.memory: 2Gi → 512Mi
2026-08-28 14:02:58.001  Added     kubectl-client-side-apply  full state recorded
```

At 14:03 an apply cut the memory limit from 2Gi to 512Mi. Ninety seconds later
the controller scaled the replica set, and the pods it created stopped fitting.
**The change, the actor who made it, and the Event it caused — one command, one
screen.** No SQL, no dashboard, and no guessing which of six controllers touched
the object.

Then narrow it to exactly what moved, or pull the object back as it stood before
any of it happened:

```console
$ kubectl kuberecord diff deploy/checkout -n payments --since 30m
2026-08-28 14:03:11.482 UTC  Modified  kubectl-client-side-apply
  ~ spec.template.spec.containers[0].resources.limits.memory
      - 2Gi
      + 512Mi

$ kubectl kuberecord get deploy/checkout -n payments --at 2026-08-28T14:00:00Z
```

KubeRecord is a Kubernetes operator that streams every observed state change of
the resources you name — `Added`, `Modified`, `Deleted` — into the sink you
choose, as immutable, append-only records. `kubectl kuberecord`, above, is how
you read them back: [four ways to install it](#installing-the-cli), and it also
runs standalone against an object-store archive with no cluster at all.

So "what did this look like five minutes before it broke?" has an answer, and
answering it is a query rather than an archaeology project.

---

## Why

Kubernetes forgets. `kubectl get events` shows you roughly the last hour. A Pod
that was evicted is gone, along with the status that explains why. A Deployment
that was rolled back keeps no record of the spec that broke it. Post-mortems end
up reconstructed from memory, dashboards, and luck.

kuberecord watches a declarative set of resource types and records every state
transition. It hashes and diffs each object's normalized JSON, so it writes only
when something actually changed — a compact, queryable, retrospective timeline
rather than a live-only snapshot or a firehose of duplicates.

The watched set is never a compiled-in list. Extending coverage to another
built-in type or to your own CRDs is a custom resource and an RBAC grant — a
configuration change, applied to a running operator, with no restart.

Where those records land is a per-rule choice. A `ClickHouseSink` gives you the
queryable timeline the queries below run against; an `S3Sink` gives you a cheap,
compressed, optionally WORM-locked archive of the same stream — and one watch can
feed both at once, which is the tee pattern ([`docs/TEE.md`](docs/TEE.md), runnable
at [`examples/tee/`](examples/tee/)). What an immutable archive is worth, and
precisely what kuberecord does *not* promise about it, is
[`docs/RETENTION.md`](docs/RETENTION.md).

### This is not your audit log, and does not replace it

Both are worth having, and they answer different questions.

The Kubernetes **audit log** records *API calls*: who called `PATCH` on what,
from which IP, with which user agent, and whether it was allowed. It is the
authoritative record of **intent and authorization** — and it is the right tool
for "who did this?" and "should they have been able to?".

kuberecord records *resulting object state*: what the object actually looked
like afterwards, and precisely what changed. It is the record of **outcome** —
the right tool for "what was the spec at 03:14?", "what changed in this
namespace during the incident window?", and "what did this deleted object
contain?". It also captures changes no human ever made an API call for: a
controller reconciling, a status field moving, an autoscaler acting.

The complementarity is the point. An audit log entry tells you a `PATCH`
happened; it does not tell you what the object became, and reconstructing that
from a request body across many controllers writing to one object is not
practical. kuberecord tells you what the object became; it does not tell you who
was authenticated. Run both, and "who changed it" joins to "what it became" on
time and object identity. `actors` — the field managers owning parts of the
object — is the bridge, with the honest caveat that it records *ownership at
write time*, not authorship of one edit.

Where audit logs and kuberecord genuinely overlap, kuberecord is the cheaper
place to keep history: rows are deduplicated and diffed, so an object that
changes once a week costs one row a week regardless of how many controllers
touched it.

## Quickstart

From a fresh clone to rows you can query, on a laptop, in under ten minutes.
You need Docker, [kind] and `kubectl`.

```sh
git clone https://github.com/kuberecord/kuberecord && cd kuberecord
make quickstart
```

That builds the operator from your clone, stands up a kind cluster with a
single-node ClickHouse, applies a sink and a rule, creates a demo workload,
changes it, and then **queries the rows it recorded** and prints them — including
the diff behind a scale-up and proof that a redacted value never reached the
database. It ends by printing the port-forward and a first query so you can keep
going. `make quickstart-down` deletes all of it.

The ten-minute claim is tested rather than asserted: CI runs the same script with
a hard 600-second budget on every push, and the run fails if no rows arrive or if
it overruns.

```console
$ make quickstart
...
==> [01:36] Rows recorded (cluster_id = 'kuberecord-quickstart')
   ┌─event_type─┬─kind───────┬─namespace───────┬─name────────────┬────────────────────────────ts─┐
1. │ Snapshot   │ ConfigMap  │ quickstart-demo │ checkout-config │ 2026-08-03 00:59:56.199673090 │
2. │ Snapshot   │ Deployment │ quickstart-demo │ checkout-api    │ 2026-08-03 00:59:56.199748632 │
3. │ Modified   │ ConfigMap  │ quickstart-demo │ checkout-config │ 2026-08-03 00:59:58.458432966 │
   └────────────┴────────────┴─────────────────┴─────────────────┴───────────────────────────────┘

==> [01:37] The feature-flag change, as a diff
diff: [{"value":"new-checkout=on","op":"replace","path":"/data/feature_flags"}]

==> [01:37] Redaction: the ConfigMap's password never reached the database
   ┌─name────────────┬─feature_flags────┬─password───┐
1. │ checkout-config │ new-checkout=off │ [REDACTED] │
   └─────────────────┴──────────────────┴────────────┘

kuberecord is streaming. 8 rows in 96s.
```

Every step is an ordinary `kubectl apply` you can run by hand, and
[`examples/quickstart/`](examples/quickstart/) documents each one — including
which parts are evaluation shortcuts (`emptyDir` storage, a committed password)
and which are exactly how a production install works (all of the RBAC, all of the
pipeline).

### Or without a database at all

Standing up a database to decide whether a tool is worth having is a bad trade.
This path removes it: one `helm install`, an object store, and the answer read
back with `kubectl kuberecord` — no ClickHouse anywhere.

```sh
make quickstart-zero-infra
```

It stands up a kind cluster, **installs the operator with Helm**, applies an
`S3Sink` pointed at a single-node MinIO, records a demo workload changing, and
then reads that history back with the CLI — `scopes`, `timeline`, `diff` and a
reconstruction proving a redacted value never reached the archive. The last step
runs the same query again with no kubeconfig at all, because an archive needs no
cluster to be read. `make quickstart-zero-infra-down` deletes all of it.

Same ten-minute claim, tested the same way: CI runs this script too, on every
push, with a hard 600-second budget, and the run fails if the CLI reads no changes
out of the archive or if the whole thing overruns.

Nothing about the *capture* path is reduced by it — an `S3Sink` records what a
`ClickHouseSink` records. What is different is the read side, it is declared
rather than discovered, and the CLI says so where it applies: an archive has no
index, no deletions and needs a time bound, and each of those changes what the
output tells you. The whole table is
[`docs/CLI.md`](docs/CLI.md#backend-capability-differences), and
[`examples/zero-infra/`](examples/zero-infra/) documents every step of this one.

Adding a queryable timeline later is not a migration: a rule targets exactly one
sink permanently, so it is a `ClickHouseSink` and a **second rule** beside this
one — the tee pattern, [`docs/TEE.md`](docs/TEE.md).

## Your first five queries

Everything below runs against the frozen v1 schema and uses ClickHouse-native
parameters, so it is copy-pasteable without editing:

```console
$ clickhouse-client --param_cluster=kuberecord-quickstart \
    --param_namespace=payments --param_from='2026-08-01 13:45:00.000' \
    --param_to='2026-08-01 14:30:00.000' --queries-file first.sql
```

**1. What has been happening?** The whole stream, newest first. Start here.

```sql
SELECT
    ts,
    event_type,
    kind,
    namespace,
    name,
    arrayStringConcat(arraySort(actors), ', ') AS field_managers
FROM resource_states
WHERE cluster_id = {cluster:String}
ORDER BY ts DESC
LIMIT 20;
```

**2. The incident window.** *"Payments broke between 13:45 and 14:30 — what
moved?"* Diffs are inlined, so this is the whole answer rather than a list of
things to go and look up.

```sql
SELECT
    ts,
    event_type,
    kind,
    name,
    multiIf(diff != '', diff, data != '', '(full state)', '') AS change
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND namespace = {namespace:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
ORDER BY ts ASC;
```

**3. What did this object look like before it broke?** The newest full state
recorded at or before an instant. `Modified` rows carry diffs, so this is the
baseline you replay them onto.

```sql
SELECT ts, event_type, data
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND kind      = {kind:String}
  AND namespace = {namespace:String}
  AND name      = {name:String}
  AND data != ''
  AND ts <= {at:DateTime64(3, 'UTC')}
ORDER BY ts DESC
LIMIT 1;
```

**4. What changed that Git does not know about?** Every row carries the field
managers that owned the object at write time, so excluding your GitOps
controller's manager leaves the drift it did not cause.

```sql
SELECT
    namespace,
    kind,
    name,
    count() AS modifications,
    max(ts) AS last_change
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
  AND event_type IN ('Modified', 'Checkpoint')
  AND NOT has(actors, {manager:String})
GROUP BY namespace, kind, name
ORDER BY modifications DESC
LIMIT 20;
```

**5. What will not hold still?** Flapping is usually two controllers fighting
over one field, and the distinct managers on the object are the fight.

```sql
SELECT
    namespace,
    kind,
    name,
    count() AS modifications,
    arrayStringConcat(arraySort(groupUniqArrayArray(actors)), ', ') AS field_managers
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
  AND event_type IN ('Modified', 'Checkpoint')
GROUP BY namespace, kind, name
HAVING modifications >= {threshold:UInt32}
ORDER BY modifications DESC
LIMIT 20;
```

These five are **fleet questions** — many objects, one window — and SQL is the
right tool for them. For the questions about *one* object, the CLI at the top of
this page answers them without a query: `timeline` for query 1 narrowed to an
object, `diff` for query 2, `get --at` for query 3, and `blame` for "who last set
this field". Which is which, and why the two are complements rather than
alternatives, is [`docs/QUERIES.md`](docs/QUERIES.md#cli-or-sql).

The full library — reconstructing an object's exact state at an instant, what a
deleted object last contained, Kubernetes Events for an object around a time — is
[`docs/QUERIES.md`](docs/QUERIES.md), and every statement there is executed
against a real ClickHouse in CI. The same questions come pre-built as Grafana
dashboards: [`docs/DASHBOARDS.md`](docs/DASHBOARDS.md).

> If a value reads `[REDACTED]`, that is what is **stored**. Redaction happens
> before hashing, so a scrubbed value is absent from `data`, from every `diff`,
> and from the `sha256` — not hidden at query time.

## Architecture

Two tiers, in the Prometheus-Operator shape: a control plane that decides what
*should* be watched, and a data plane that makes reality match.

```text
  ClickHouseSink ─┐                        CONTROL PLANE
  S3Sink ─────────┤
  StreamRule ─────┼──▶  reconcilers ── validate intent · resolve credentials
  ClusterStreamRule┘         │           check RBAC (SelfSubjectAccessReview)
                             │           report status conditions
                             ▼
                  desired-state registry        ← the only thing the two share
                             │
                             │  level-triggered: debounced diff + 30s safety tick
                             ▼                    DATA PLANE
       WatchManager ── one dynamic informer per (resource, namespace),
                       started and stopped at runtime, shared across rules
                             │  Added / Modified / Deleted
                             ▼
       workqueue ── identity keys; one key is never on two workers at once
                             │
                             ▼
       pipeline workers ×N ── normalize → redact → SHA-256 → dedup
                                       → diff → reserve a cache version
                             │  bounded, non-blocking hand-off
                             ▼
       sink writer ── batched inserts or rotated objects, backoff retries,
                      version-gated commit
                             │
       ┌─────────────────────┴─────────────┐
       ▼                                   ▼
       ClickHouse ── resource_states       S3 ── format=jsonl-v1/…/*.jsonl.zst
                      · watch_scopes             · scopes/
```

Four properties are worth knowing before you read anything else:

- **Nothing blocks the hot path.** No informer handler, worker or reconciler ever
  waits on a sink. A slow or unreachable backend costs a status condition and a
  growing queue, never a stalled watch — and never a restart.
- **A stopped watch is not a deletion.** When the last rule wanting a scope goes
  away, that is recorded in `watch_scopes` as `Stopped` — and **no** `Deleted`
  rows are written for the objects it covered. "We stopped looking" and "it was
  deleted" are different truths and leave different traces.
- **Writes are version-gated.** Every write reserves a monotonic version in the
  per-sink dedup cache and only commits if that version is still current, so a
  failed write is never mistaken for a persisted one and a late acknowledgement
  can never clobber a newer state.
- **One bad rule degrades only itself.** A missing RBAC grant, a CRD that is not
  installed, an unreachable backend: each surfaces as a condition on the owning
  custom resource. Every other rule keeps streaming, and the fix applies to a
  running operator.

The [`docs/`](docs/) index below goes into each of these properly.

## Installing

Three paths, all installing the same operator — the same object names, the same
permissions, the same container arguments. `test/chart` asserts that equivalence
object by object, and the acceptance suite runs against each of them unmodified.

```sh
# Helm, from the chart registry — no checkout, no download
helm install kuberecord oci://ghcr.io/kuberecord/charts/kuberecord \
  --version 0.3.1 \
  --namespace kuberecord-system --create-namespace \
  --set clusterID=prod-eu-west-1

# A single, committed manifest
kubectl apply -f dist/install.yaml

# Kustomize, which is also the development path
make deploy IMG=<some-registry>/kuberecord:tag
```

The chart's tag carries no `v` — a Helm chart version is plain semver — and it
tracks the operator release exactly, so `--version 0.3.1` installs `v0.3.1`. It is
published from v0.3.0 onward; earlier tags ship the chart as a release asset only.

Both artifacts are also attached to every [release](https://github.com/kuberecord/kuberecord/releases),
with checksums, if you would rather download a tag than pull one:

```sh
kubectl apply -f https://github.com/kuberecord/kuberecord/releases/download/v0.3.1/install.yaml
```

And the chart in this repository installs directly, which is what a contributor
working on it does:

```sh
helm install kuberecord deploy/charts/kuberecord \
  --namespace kuberecord-system --create-namespace
```

From v0.2.0 the image is signed with cosign, the image and every attached asset
carry SLSA build provenance, and an SBOM ships beside them — worth checking before
you run an operator that will watch your whole cluster. From v0.3.0 the chart in
the registry is signed too, under the same identity:

```sh
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity https://github.com/kuberecord/kuberecord/.github/workflows/release.yml@refs/tags/v0.2.1 \
  ghcr.io/kuberecord/kuberecord:v0.2.1

# The chart. The identity keeps the `v`; the artifact's tag does not.
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity https://github.com/kuberecord/kuberecord/.github/workflows/release.yml@refs/tags/v0.3.0 \
  ghcr.io/kuberecord/charts/kuberecord:0.3.0
```

The identity above is the one to pin from **v0.2.1** onward. `v0.1.0` and `v0.2.0`
were released before this repository moved to the `kuberecord` organization, so
they verify against the old identity and their images stay at the old registry
path — see
[`docs/VERIFYING.md`](docs/VERIFYING.md#which-identity-verifies-which-release).

[`docs/VERIFYING.md`](docs/VERIFYING.md) has the rest — provenance, the SBOM,
checksums, and what a signature on the operator does *not* say about the records
it writes.

The operator is pre-1.0 — while it is `v0.x` a minor bump may break, and every
break is spelled out in [`CHANGELOG.md`](CHANGELOG.md). The ClickHouse schema
carries no such caveat: it is frozen at `v1`. The three version numbers and what
each promises are in [`docs/RELEASING.md`](docs/RELEASING.md).

Set the cluster identifier — `clusterID` for Helm, `CLUSTER_ID` on the Deployment
otherwise. It is stamped on every row, and rows already written keep the old
value.

The operator then boots **healthy and completely idle**. It starts streaming when
a sink and a rule appear, with no restart:

```yaml
apiVersion: kuberecord.io/v1alpha1
kind: ClickHouseSink
metadata: {name: default}
spec:
  connection:
    addr: clickhouse.kuberecord-system.svc:9000
    database: kuberecord
    username: kuberecord
    credentialsSecretRef: {name: clickhouse-credentials}   # operator's namespace
---
apiVersion: kuberecord.io/v1alpha1
kind: StreamRule
metadata: {name: payments-workloads, namespace: payments}
spec:
  sink: {kind: ClickHouseSink, name: default}   # a name is unique within a kind
  resources:
  - {group: apps, version: v1, kind: Deployment}
```

Two things that catch people out, both documented properly in
[`docs/CRDS.md`](docs/CRDS.md): the Secret must live in the **operator's own**
namespace (that is the only place its RBAC allows it to read one), and a rule can
only stream what the operator has been granted — apply the matching preset from
[`config/rbac/presets/`](config/rbac/presets/), or your own labelled
`ClusterRole`, and the rule activates on its own.

The ClickHouse tables come from [`deploy/clickhouse/schema/`](deploy/clickhouse/schema/).
Apply the two `.sql` files yourself, or start the operator with
`--ch-auto-create-schema=true`. **Schema v1 is frozen**: within `v1` no column is
renamed, retyped or removed, and changes are additive only.

### Uninstalling

```sh
helm uninstall kuberecord -n kuberecord-system   # leaves the CRDs behind
kubectl delete -f dist/install.yaml
make undeploy                                    # kustomize: operator + CRDs
```

Deleting the CRDs deletes every sink and rule, but not a single row: the sink is
the durable store, and nothing in an uninstall touches ClickHouse.

## Installing the CLI

The operator writes the rows; `kubectl kuberecord` is how you read them. It ships
as one binary under two names — `kubectl-kuberecord`, which makes
`kubectl kuberecord …` work, and `kuberecord`, which runs standalone against an
object-store archive with no cluster at all.

```sh
# 1. krew, which is how a kubectl user finds a plugin.
kubectl krew install kuberecord
kubectl kuberecord timeline deploy/checkout -n payments

# 2. Homebrew, on macOS and on Linux. The one channel that installs both names.
brew install kuberecord/tap/kuberecord

# 3. The release archive, directly — five platforms, Windows included.
curl -fsSLO https://github.com/kuberecord/kuberecord/releases/download/v0.3.1/kuberecord_v0.3.1_linux_amd64.tar.gz
tar -xzf kuberecord_v0.3.1_linux_amd64.tar.gz
install -m 0755 kubectl-kuberecord kuberecord ~/.local/bin/

# 4. From source, with a Go toolchain.
go install github.com/kuberecord/kuberecord/cmd/kubectl-kuberecord@v0.3.1
```

krew installs the plugin name only — that is what a plugin manager is for — so
the standalone `kuberecord` comes from Homebrew or from the archive. `go install`
compiles rather than downloads: no standalone name, no release stamp, and nothing
signed. Which channel gives you what is a table in
[`docs/CLI.md`](docs/CLI.md#installing).

The archives are cross-compiled cgo-free, carry both names from one compilation,
and are signed through `checksums.txt` — see
[`docs/VERIFYING.md`](docs/VERIFYING.md#the-cli-archives). The CLI ships from
v0.3.0 onward; earlier releases have no archives to install.

## Documentation

| Page | What is in it |
|---|---|
| [`docs/SCHEMA.md`](docs/SCHEMA.md) | What is stored, in three parts: the backend-independent record contract (the `event_type` state machine, the RFC 6902 diff format, checkpoints and state reconstruction, redaction, the version-agnostic identity rule), then the **frozen v1 ClickHouse schema** column by column, then the **`jsonl-v1` S3 object format** and its key layout. |
| [`docs/QUERIES.md`](docs/QUERIES.md) | The query library, for both backends. Incident windows, drift by actor, flap reports, state reconstruction, Events for an object, what a deleted object last contained — plus DuckDB recipes and an Athena table for the S3 archive. Every ClickHouse statement is executed against a real ClickHouse in CI, and every DuckDB recipe against a real object store. |
| [`docs/RBAC.md`](docs/RBAC.md) | The aggregated-ClusterRole model, the no-self-escalation argument, granting a new kind in 30 seconds, and the honest read-flattening caveat. |
| [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) | Measured envelopes per scale profile — throughput, p99 enqueue-block, CPU and RSS at up to 20,000 watched objects — and how to reproduce them. |
| [`docs/CLI.md`](docs/CLI.md) | The `kubectl kuberecord` reference. Every command — `timeline`, `diff`, `get --at`, `blame`, `scopes`, `version`, `config` — with every flag, the exit codes, the output formats, and the `cli.kuberecord.io/v1alpha1` envelope that makes `-o json` a contract rather than a rendering. Then where it reads from (the `--source` / `--sink` / profile / discovered-sink chain), how the `cluster_id` is resolved, what each backend can and cannot answer, what a cold scan of an unindexed archive costs, the configuration file schema — which references a password and never stores one — and the read-only ClickHouse user that is the recommended posture for querying. |
| [`docs/CRDS.md`](docs/CRDS.md) | Every field of the three custom resources, what each validation rejects and why, and every status condition they report. |
| [`docs/TEE.md`](docs/TEE.md) | Hot and cold: a queryable ClickHouse timeline and a cheap immutable object-store archive, from one watch. Why the answer is two rules rather than one clever sink, why one informer serves both, why dedup state is per sink — and exactly which guarantees the archive half does not carry. Runnable: [`examples/tee/`](examples/tee/). |
| [`docs/RETENTION.md`](docs/RETENTION.md) | Tamper-evidence and retention: enabling S3 Object Lock (a bucket prerequisite kuberecord cannot set), what `spec.objectLock` applies per object, `GOVERNANCE` versus `COMPLIANCE`, how lifecycle rules interact with a locked archive — and an explicit limits section, because kuberecord signs nothing and redaction is forward-only. |
| [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) | Operator flags and environment variables, and how the `--writer-*` fallbacks relate to a sink's own fields. |
| [`docs/OPERATING.md`](docs/OPERATING.md) | Watching the operator: every exported metric, the shipped dashboard and alerts, and a runbook per alert. |
| [`docs/DASHBOARDS.md`](docs/DASHBOARDS.md) | The four ClickHouse-reading Grafana dashboards, panel by panel. |
| [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) | Building, the make targets, and what each test suite proves. |
| [`docs/RELEASING.md`](docs/RELEASING.md) | What a version number promises: the operator's `v0.x` (pre-1.0, a minor may break), the CRDs' `v1alpha1`, and the frozen schema `v1` — three numbers that move independently. Plus what a tagged release publishes and how to cut one. |
| [`docs/VERIFYING.md`](docs/VERIFYING.md) | Checking a release before you run it: the keyless `cosign verify` command with the issuer and identity to pin, the SLSA provenance attestation for the image and for every asset, the SBOM, and `checksums.txt`. Plus the limit that matters — a signed *operator* is not a signed *audit trail*. |
| [`docs/UPGRADING.md`](docs/UPGRADING.md) | What to do when a `v0.x` minor breaks: the version-by-version upgrade steps, with the exact `kubectl` sequence for each. |
| [`CHANGELOG.md`](CHANGELOG.md) | Including the migration table from the removed environment-variable configuration to the custom resources that replaced it. |

## Use cases

- **Incident post-mortems** — the exact spec and status an object had at the
  moment things went wrong, instead of hoping someone screenshotted it.
- **GitOps forensics** — configuration drift across a fleet: what changed, when,
  and which field manager owned it.
- **SecOps** — an operator-owned record of resource state that does not depend on
  the API server's audit retention window.
- **Compliance** — a durable, timestamped history of workload state changes, per
  cluster, for retrospective review. Where retention outlives what is worth
  keeping queryable, stream the same resources to two backends at once: a
  ClickHouse timeline for the questions people ask, and a WORM-capable object
  store for the years nobody reads. That is the **tee pattern**,
  [`docs/TEE.md`](docs/TEE.md) — one watch, two rules, no extra load on the API
  server. What "WORM-capable" is worth, precisely — and what it is not, since
  kuberecord signs nothing — is [`docs/RETENTION.md`](docs/RETENTION.md).

## Development

`make help` lists every target; [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)
explains the suites.

```sh
make build test lint      # build, unit + envtest suite, golangci-lint
make test-integration     # against throwaway dockerized ClickHouse and MinIO
make test-e2e             # the acceptance suite on a kind cluster
make test-chaos           # the failure-mode suite: outages, SIGKILL, saturation
make quickstart           # the evaluation path above
make quickstart-zero-infra  # the same, with an object store and no database
```

## Contributing

Issues and pull requests are welcome.
[`CONTRIBUTING.md`](CONTRIBUTING.md) covers the setup, the development loop and
what a reviewable change looks like here; participation is governed by the
[Code of Conduct](CODE_OF_CONDUCT.md).

Security vulnerabilities go to [`SECURITY.md`](SECURITY.md) — privately, never to
a public issue.

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

[kind]: https://kind.sigs.k8s.io/docs/user/quick-start/#installation
