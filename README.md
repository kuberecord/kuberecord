# kubestream

**Git blame for your Kubernetes cluster.**

A Kubernetes operator that streams every observed state change of the resources
you name — `Added`, `Modified`, `Deleted` — into ClickHouse as immutable,
append-only rows. So "what did this look like five minutes before it broke?" has
an answer, and answering it is a query rather than an archaeology project.

```console
$ make quickstart
...
==> [01:36] Rows recorded (cluster_id = 'kubestream-quickstart')
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

kubestream is streaming. 8 rows in 96s.
```

---

## Why

Kubernetes forgets. `kubectl get events` shows you roughly the last hour. A Pod
that was evicted is gone, along with the status that explains why. A Deployment
that was rolled back keeps no record of the spec that broke it. Post-mortems end
up reconstructed from memory, dashboards, and luck.

kubestream watches a declarative set of resource types and records every state
transition. It hashes and diffs each object's normalized JSON, so it writes only
when something actually changed — a compact, queryable, retrospective timeline
rather than a live-only snapshot or a firehose of duplicates.

The watched set is never a compiled-in list. Extending coverage to another
built-in type or to your own CRDs is a custom resource and an RBAC grant — a
configuration change, applied to a running operator, with no restart.

### This is not your audit log, and does not replace it

Both are worth having, and they answer different questions.

The Kubernetes **audit log** records *API calls*: who called `PATCH` on what,
from which IP, with which user agent, and whether it was allowed. It is the
authoritative record of **intent and authorization** — and it is the right tool
for "who did this?" and "should they have been able to?".

kubestream records *resulting object state*: what the object actually looked
like afterwards, and precisely what changed. It is the record of **outcome** —
the right tool for "what was the spec at 03:14?", "what changed in this
namespace during the incident window?", and "what did this deleted object
contain?". It also captures changes no human ever made an API call for: a
controller reconciling, a status field moving, an autoscaler acting.

The complementarity is the point. An audit log entry tells you a `PATCH`
happened; it does not tell you what the object became, and reconstructing that
from a request body across many controllers writing to one object is not
practical. kubestream tells you what the object became; it does not tell you who
was authenticated. Run both, and "who changed it" joins to "what it became" on
time and object identity. `actors` — the field managers owning parts of the
object — is the bridge, with the honest caveat that it records *ownership at
write time*, not authorship of one edit.

Where audit logs and kubestream genuinely overlap, kubestream is the cheaper
place to keep history: rows are deduplicated and diffed, so an object that
changes once a week costs one row a week regardless of how many controllers
touched it.

## Quickstart

From a fresh clone to rows you can query, on a laptop, in under ten minutes.
You need Docker, [kind] and `kubectl`.

```sh
git clone https://github.com/yelzhy/kuberecord && cd kubestream
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

Every step is an ordinary `kubectl apply` you can run by hand, and
[`examples/quickstart/`](examples/quickstart/) documents each one — including
which parts are evaluation shortcuts (`emptyDir` storage, a committed password)
and which are exactly how a production install works (all of the RBAC, all of the
pipeline).

## Your first five queries

Everything below runs against the frozen v1 schema and uses ClickHouse-native
parameters, so it is copy-pasteable without editing:

```console
$ clickhouse-client --param_cluster=kubestream-quickstart \
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
       sink writer ── batched inserts, backoff retries, version-gated commit
                             │
                             ▼
                    ClickHouse ── resource_states · watch_scopes
```

Four properties are worth knowing before you read anything else:

- **Nothing blocks the hot path.** No informer handler, worker or reconciler ever
  waits on ClickHouse. A slow or absent database costs a status condition and a
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
# Helm
helm install kubestream deploy/charts/kubestream \
  --namespace kubestream-system --create-namespace \
  --set clusterID=prod-eu-west-1

# A single, committed manifest
kubectl apply -f dist/install.yaml

# Kustomize, which is also the development path
make deploy IMG=<some-registry>/kubestream:tag
```

Both artifacts are also attached to every [release](https://github.com/yelzhy/kuberecord/releases),
with checksums, if you would rather install a tag than a checkout:

```sh
kubectl apply -f https://github.com/yelzhy/kuberecord/releases/download/v0.1.0/install.yaml
```

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
    addr: clickhouse.kubestream-system.svc:9000
    database: kubestream
    username: kubestream
    credentialsSecretRef: {name: clickhouse-credentials}   # operator's namespace
---
apiVersion: kuberecord.io/v1alpha1
kind: StreamRule
metadata: {name: payments-workloads, namespace: payments}
spec:
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
helm uninstall kubestream -n kubestream-system   # leaves the CRDs behind
kubectl delete -f dist/install.yaml
make undeploy                                    # kustomize: operator + CRDs
```

Deleting the CRDs deletes every sink and rule, but not a single row: the sink is
the durable store, and nothing in an uninstall touches ClickHouse.

## Documentation

| Page | What is in it |
|---|---|
| [`docs/SCHEMA.md`](docs/SCHEMA.md) | The frozen v1 schema, column by column: the `event_type` state machine, the RFC 6902 diff format, checkpoint rows and state reconstruction, redaction, the version-agnostic identity rule, delivery semantics. |
| [`docs/QUERIES.md`](docs/QUERIES.md) | The query library. Incident windows, drift by actor, flap reports, state reconstruction, Events for an object, what a deleted object last contained — every statement executed against a real ClickHouse in CI. |
| [`docs/RBAC.md`](docs/RBAC.md) | The aggregated-ClusterRole model, the no-self-escalation argument, granting a new kind in 30 seconds, and the honest read-flattening caveat. |
| [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) | Measured envelopes per scale profile — throughput, p99 enqueue-block, CPU and RSS at up to 20,000 watched objects — and how to reproduce them. |
| [`docs/CRDS.md`](docs/CRDS.md) | Every field of the three custom resources, what each validation rejects and why, and every status condition they report. |
| [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) | Operator flags and environment variables, and how the `--writer-*` fallbacks relate to a sink's own fields. |
| [`docs/OPERATING.md`](docs/OPERATING.md) | Watching the operator: every exported metric, the shipped dashboard and alerts, and a runbook per alert. |
| [`docs/DASHBOARDS.md`](docs/DASHBOARDS.md) | The four ClickHouse-reading Grafana dashboards, panel by panel. |
| [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) | Building, the make targets, and what each test suite proves. |
| [`docs/RELEASING.md`](docs/RELEASING.md) | What a version number promises: the operator's `v0.x` (pre-1.0, a minor may break), the CRDs' `v1alpha1`, and the frozen schema `v1` — three numbers that move independently. Plus what a tagged release publishes and how to cut one. |
| [`CHANGELOG.md`](CHANGELOG.md) | Including the migration table from the removed environment-variable configuration to the custom resources that replaced it. |

## Use cases

- **Incident post-mortems** — the exact spec and status an object had at the
  moment things went wrong, instead of hoping someone screenshotted it.
- **GitOps forensics** — configuration drift across a fleet: what changed, when,
  and which field manager owned it.
- **SecOps** — an operator-owned record of resource state that does not depend on
  the API server's audit retention window.
- **Compliance** — a durable, timestamped history of workload state changes, per
  cluster, for retrospective review.

## Development

`make help` lists every target; [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)
explains the suites.

```sh
make build test lint      # build, unit + envtest suite, golangci-lint
make test-integration     # against a throwaway dockerized ClickHouse
make test-e2e             # the acceptance suite on a kind cluster
make test-chaos           # the failure-mode suite: outages, SIGKILL, saturation
make quickstart           # the evaluation path above
```

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
